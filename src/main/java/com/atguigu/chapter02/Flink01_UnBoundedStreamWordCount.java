package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink01_WordCount
 * @Description TODO
 * @Author hcf
 * @Date 2021/9/14 17:50
 * @Version 1.0
 * 批处理
 */
public class Flink01_UnBoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        socketDS
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                })
                .keyBy(r -> r.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
/**
 * 用lambda时候泛型的类型擦除问题  map flatmap
 * 要加returns(Types.of())指定泛型的类型
 *
 */