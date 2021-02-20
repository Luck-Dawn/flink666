package com.zky.chapter05;

import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author：Dawn
 * @date：2020/12/15 20:48
 * @Desc： 实现网站pv统计
 */
public class Flink24_Case_PVByProcess {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4]));
                    }
                });

        //过滤出数据为pv的
        SingleOutputStreamOperator<UserBehavior> pvDS = userBehaviorDS.filter(r -> "pv".equals(r.getBehavior()));

        KeyedStream<UserBehavior, String> keyedDS = pvDS.keyBy(r -> r.getBehavior());

        SingleOutputStreamOperator<Long> resultDS = (SingleOutputStreamOperator<Long>) keyedDS.process(new KeyedProcessFunction<String, UserBehavior, Long>() {
            private long count = 0L;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                count++;
                out.collect(count);

            }
        });

        resultDS.print("pv");

        env.execute();
    }
}
