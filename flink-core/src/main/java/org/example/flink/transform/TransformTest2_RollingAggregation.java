package org.example.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @author: wyk
 * @date 2021/5/31 10:44
 * @Description 滚动聚合算子Rolling Aggregation测试
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从数据源获取数据
        DataStream<String> dataStream = env.readTextFile("data/sensor.txt");

        //map 操作：匿名内部类方式
       /* DataStream<SensorReading> sensorStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });*/

        //map 操作：lambda方式
        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //先分组再聚合
        //分组
        KeyedStream<SensorReading, String> keyedStream = sensorStream.keyBy(SensorReading::getId);

        //滚动聚合,max和maxBy区别在于，maxBy除了用于max比较的字段以外，其他字段也会更新成最新的，而max只有比较的字段更新，其他字段不变
        //DataStream<SensorReading> resultStream = keyedStream.max("temperature");
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        //打印结果
        resultStream.print();

        //执行
        env.execute();
    }
}
