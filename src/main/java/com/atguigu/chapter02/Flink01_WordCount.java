package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink01_WordCount
 * @Description TODO
 * @Author hcf
 * @Date 2021/9/14 17:50
 * @Version 1.0
 * 批处理
 *
 */
public class Flink01_WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> inputDS = env.readTextFile("input/word.txt");
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneDS = inputDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }

                    }
                });
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneDS.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> result = wordAndOneGroup.sum(1);

        result.print();

        env.execute();
    }
}
