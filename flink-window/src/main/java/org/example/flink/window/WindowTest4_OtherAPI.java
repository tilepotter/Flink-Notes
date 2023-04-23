package org.example.flink.window;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName WindowTest4_OtherAPI
 * @Author wangyingkang
 * @Date 2021/7/19 14:57
 * @Version 1.0
 * @Description 其他可选API测试
 **/
public class WindowTest4_OtherAPI {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度为1，好观察测试输出结果
        env.setParallelism(1);

        //3.从socket流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        //4.转成Sensording类型
        DataStream<SensorReading> mapStream = inputStream.map(s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late");


        SingleOutputStreamOperator<SensorReading> sumStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(10))
//                .trigger() // 触发器，一般不使用
//                .evictor()//移除器，一般不使用
                .allowedLateness(Time.minutes(1))//允许一分钟内的迟到数据，比如数据产生时间在窗口范围内，但是要处理的时候已经超过窗口时间了
                .sideOutputLateData(outputTag)// 侧输出流，迟到超过1分钟的数据，收集于此
                .sum("temperature");// 侧输出流 对 温度信息 求和。


        DataStream<SensorReading> sideOutputStream = sumStream.getSideOutput(outputTag);

        // 之后可以再用别的程序，把侧输出流的信息和前面窗口的信息聚合。（可以把侧输出流理解为用来批处理来补救处理超时数据）

    }
}
