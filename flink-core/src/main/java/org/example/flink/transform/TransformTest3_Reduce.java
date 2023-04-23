package org.example.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @author: wyk
 * @date 2021/6/11 10:45
 * @Description  在前面Rolling Aggregation的前提下，对需求进行修改。获取同组历史温度最高的传感器信息，同时要求实时更新其时间戳信息。
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从数据源读取数据
        DataStreamSource<String> dataStream = env.readTextFile("data/sensor.txt");

        //4.map操作
        DataStream<SensorReading> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //5.先分组在聚合
        //5.1分组
        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(SensorReading::getId);

        //5.2 reduce聚合(匿名内部类方式)
        /*DataStream<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading currentSensor, SensorReading newSensor) throws Exception {
                return new SensorReading(currentSensor.getId(), newSensor.getTimestamp(),
                        Math.max(currentSensor.getTemperature(), newSensor.getTemperature()));
            }
        });*/

        //5.2 reduce聚合(lambda方式)
        DataStream<SensorReading> reduceStream = keyedStream.reduce(
                (currentSensor, newSensor) -> new SensorReading(currentSensor.getId(), newSensor.getTimestamp(),
                        Math.max(currentSensor.getTemperature(), newSensor.getTemperature()))
        );

        //6.打印
        reduceStream.print("result");

        //7.执行
        env.execute();
    }
}
