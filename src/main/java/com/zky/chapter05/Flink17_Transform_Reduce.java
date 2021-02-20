package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author：Dawn
 * @date：2020/11/27 10:31
 * @Desc：
 */
public class Flink17_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //第一条sensor流
        KeyedStream<Tuple3<String, Long, Integer>, String> keyedDS = env.readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new Tuple3<>(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
                    }
                })
                .keyBy(r -> r.f0);


        // TODO Reduce
        // 1.输入的类型要一致，输出的类型也要一致
        // 2.第一条来的数据，不会进入reduce
        // 3.帮我们保存了中间状态
        keyedDS
                .reduce(
                        new ReduceFunction<Tuple3<String, Long, Integer>>() {
                            @Override
                            public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
                                System.out.println("测试！！" + value1.toString() + " <-> " + value2.toString());
                                return Tuple3.of("aaa", 123L, value1.f2 + value2.f2);
                            }
                        }
                )
                .print("reduce");


        env.execute();
    }
}
