package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //connection.setAutoCommit(true);
    }

    //value:{"db":"","tableName":"","before":{},"after":{"id":""...},"type":"","sinkTable":""}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        try {

            //拼接SQL语句:upsert into db.tn(id,name,sex) values(1001,zhangsan,male)
            String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("after"));
            System.out.println(upsertSql);

            //编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行
            preparedStatement.execute();

            //提交
            connection.commit();

        } catch (SQLException e) {
            System.out.println("写入" + value.getString("sinkTable") + "表数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
    private String genUpsertSql(String sinkTable, JSONObject after) {

        //获取列名  [id,name,sex]  =>  "id,name,sex"      mkString(list,",")
        Set<String> columns = after.keySet();

        //获取列值
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
    }
}
