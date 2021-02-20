package com.zky.chapter05;

import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author：Dawn
 * @date：2020/12/15 20:48
 * @Desc： 实现网站pv统计
 */
public class Flink23_Case_PV {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> pvAndOneDS = pvDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {

                return Tuple2.of("pv", 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedDS = pvAndOneDS.keyBy(r -> r.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = keyedDS.sum(1);

        resultDS.print("pv");

        env.execute();
    }
}
