package com.zky.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink26_Case_UV {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<Tuple2<String, String>> uvDS = env
                .readTextFile("input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                        String[] datas = value.split(",");
                        if ("pv".equals(datas[3])) {
                            out.collect(Tuple2.of("uv", datas[0]));
                        }
                    }
                });

        KeyedStream<Tuple2<String, String>, String> keyedDS = uvDS.keyBy(r -> r.f0);
        keyedDS.timeWindow()
        keyedDS.assignTimestampsAndWatermarks()

        //uv 和pv 的区别就是在pv的的基础上按照用户进行分组去重
        SingleOutputStreamOperator<Integer> resultDS = keyedDS.process(new KeyedProcessFunction<String, Tuple2<String, String>, Integer>() {
            private Set<String> userSet = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<Integer> out) throws Exception {
                String userId = value.f1;
                userSet.add(userId);
                out.collect(userSet.size());
            }
        });

        resultDS.print();


        env.execute();
    }

}
