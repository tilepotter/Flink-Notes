package org.example.flink.source;

import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

import java.util.Arrays;

/**
 * @author: wyk
 * @date 2021/5/26 16:35
 * @Description 测试Flink从集合中读取数据
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        //1。创建执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.Source: 从集合collection中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 123456L, 34.1),
                        new SensorReading("sensor_2", 23456L, 35.8),
                        new SensorReading("sensor_3", 34567L, 36.7)));

        DataStream<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        //打印输出
        dataStream.print("SENSOR");
        intStream.print("INT");


        //执行
        env.execute("JobName");
    }
}
