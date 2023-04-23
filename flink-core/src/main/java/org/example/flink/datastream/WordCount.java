package org.example.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: wyk
 * @date 2021/4/20 11:23
 * @Description 批处理 wordcount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        //env.setMaxParallelism(4);

        //从文件中读取数据
        //String path = "F:\\java\\study\\Flink\\flink_study\\src\\main\\resources\\word.txt";
        //DataStream<String> inputDataStream = env.readTextFile(path);

        //从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        //对数据集进行处理，按空格分次展开，转换为(word,1)二元组进行统计
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);


        resultStream.print();

        //执行任务
        env.execute();
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = s.split(" ");

            //遍历所有word，包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
