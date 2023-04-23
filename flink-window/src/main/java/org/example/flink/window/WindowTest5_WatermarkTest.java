package org.example.flink.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.flink.bean.SensorReading;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName WindowTest5_WatermarkTest
 * @Author wangyingkang
 * @Date 2021/8/24 11:00
 * @Version 1.0
 * @Description 测试Watermark和迟到数据, 这里设置的Watermark的延时时间是2s，实际一般设置和window大小一致。
 * 一般大数据场景都是考虑高并发情况，所以一般使用周期性生成Watermark的方式，避免频繁地生成Watermark。
 **/
public class WindowTest5_WatermarkTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //设置事件时间
        //Flink1.12.X 已经默认使用EventTime了，所以这行代码可以隐藏
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置周期性生成watermark
        env.getConfig().setAutoWatermarkInterval(100);

        //从socket文本流读取数据
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换为SensorReading类型，分配时间戳和watermark
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                // 旧版 (新版官方推荐用assignTimestampsAndWatermarks(WatermarkStrategy) )
                //升序数据设置事件时间和watermark
                /* .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                     @Override
                     public long extractAscendingTimestamp(SensorReading sensorReading) {
                         return sensorReading.getTimestamp() * 1000L;
                     }
                 });*/


                // 旧版 (新版官方推荐用assignTimestampsAndWatermarks(WatermarkStrategy) )
                // 乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp() * 1000L;
                    }
                });

        //定义侧输出流
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15s内的最小温度值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))   //允许迟到一分钟
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
