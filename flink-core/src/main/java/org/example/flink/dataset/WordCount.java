package org.example.flink.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: wyk
 * @date 2021/4/20 11:23
 * @Description 批处理 wordcount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String path = "data/word.txt";
        DataSet<String> inputDataSet = env.readTextFile(path);

        //对数据集进行处理，按空格分次展开，转换为(word,1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);


        resultSet.print();
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
