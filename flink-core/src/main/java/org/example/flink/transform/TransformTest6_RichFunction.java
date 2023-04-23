package org.example.flink.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName TransformTest6_RichFunction
 * @Author wangyingkang
 * @Date 2021/6/21 16:24
 * @Version 1.0
 * @Description 1.“富函数”是DataStream API提供的一个函数类的接口，所有Flink函数类都有其Rich版本。
 *                  它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
 *                  RichMapFunction，RichFlatMapFunction， RichFilterFunction ....
 *              2.Rich Function有一个生命周期的概念。典型的生命周期方法有：
 *                  1).open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
 *                  2).close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
 *                  3).getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态
 **/
public class TransformTest6_RichFunction {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(4);

        //3.从textFile读取数据
        DataStreamSource<String> inputStream = env.readTextFile("data/sensor.txt");

        //4.转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        //dataStream.print();
        resultStream.print();

        env.execute();

    }

    // 传统的Function不能获取上下文信息，只能处理当前数据，不能和其他数据交互
    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 实现自定义富函数类（RichMapFunction是一个抽象类）
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }
}
