package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//数据流：web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
//程  序：Mock    -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka
public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致

        //设置状态后端
        //env.setStateBackend(new FsStateBackend(""));
        //开启CK
        //env.enableCheckpointing(5000); //生产环境设置分钟级
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.读取  Kafka dwd_page_log  主题的数据创建流
        String groupId = "unique_visit_app_210625";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程对每天的mid做去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> visitDtState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("dt-state", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                descriptor.enableTimeToLive(stateTtlConfig);
                visitDtState = getRuntimeContext().getState(descriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //获取上一跳页面
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null) {
                    //取出状态数据
                    String visitDt = visitDtState.value();

                    //取出当前数据的日期
                    String curDt = sdf.format(value.getLong("ts"));

                    if (visitDt == null || !visitDt.equals(curDt)) {
                        visitDtState.update(curDt);
                        return true;
                    }
                }

                return false;
            }
        });

        //TODO 6.将数据写出到Kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 7.启动任务
        env.execute("UniqueVisitApp");

    }

}
