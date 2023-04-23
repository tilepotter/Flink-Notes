package org.example.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName WindowTest1_TimeWindow
 * @Author wangyingkang
 * @Date 2021/7/19 10:46
 * @Version 1.0
 * @Description 时间窗口增量聚合函数测试
 * 增量聚合函数，特点即每次数据过来都处理，但是到了窗口临界才输出结果。
 **/
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度为1，好观察测试输出结果
        env.setParallelism(1);

        //3.从socket流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999, "\n", 3);

        //4.转成Sensording类型
        DataStream<SensorReading> mapStream = inputStream.map(s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //开窗测试
        //1、增量聚合函数(这里简单统计每个key组里传感器信息的总数)
        DataStream<Integer> resultstream = mapStream.keyBy("id")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    // 新建的累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 每个数据在上次的基础上累加
                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 返回结果值
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 分区合并结果(TimeWindow一般用不到，SessionWindow可能需要考虑合并)
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultstream.print();

        env.execute();
    }
}
