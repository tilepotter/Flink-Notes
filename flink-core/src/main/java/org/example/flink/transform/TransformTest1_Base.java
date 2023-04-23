package org.example.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: wyk
 * @date 2021/5/28 10:18
 * @Description 基本转换算子测试
 * map、flatMap、filter通常被统一称为基本转换算子（简单转换算子）
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度，使得任务抢占同一线程
        env.setParallelism(1);

        //3.从textfile读取数据
        DataStream<String> dataStream = env.readTextFile("data/sensor.txt");

        // 1).map string => 字符串长度int
        DataStream<Integer> mapStream = dataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        // 2).flatMap，按逗号分割字符串
        DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, String>(
        ) {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });

        // 3).filter , 筛选 sensor_1 开头的数据
        DataStream<String> filterStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        //4.打印输出
        //dataStream.print();
        mapStream.print();
        flatMapStream.print();
        filterStream.print();

        //5.执行
        env.execute();
    }

}

