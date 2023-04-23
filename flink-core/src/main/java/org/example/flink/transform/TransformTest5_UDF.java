package org.example.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName TransformTest5_UDF
 * @Author wangyingkang
 * @Date 2021/6/21 09:48
 * @Version 1.0
 * @Description 自定义KeyWordFilter类，提供有参构造方法，进行关键词过滤
 **/
public class TransformTest5_UDF {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从textFile读取数据
        DataStreamSource<String> inputStream = env.readTextFile("data/sensor.txt");

        DataStream<String> filterStream = inputStream.filter(new KeyWordFilter("35"));

        filterStream.print();

        env.execute();
    }

    /**
     * 静态类实现FilterFunction函数接口，并提供有参构造方法，给属性keyWord赋值
     */
    public static class KeyWordFilter implements FilterFunction<String> {

        private String keyWord;

        public KeyWordFilter(String keyWord) {
            this.keyWord = keyWord;
        }


        @Override
        public boolean filter(String value) throws Exception {
            return value.contains(this.keyWord);
        }
    }
}
