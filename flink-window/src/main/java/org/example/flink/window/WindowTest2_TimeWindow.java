package org.example.flink.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName WindowTest2_TimeWindow
 * @Author wangyingkang
 * @Date 2021/7/19 11:26
 * @Version 1.0
 * @Description 测试滚动时间窗口的全窗口函数
 * 全窗口函数，特点即数据过来先不处理，等到窗口临界再遍历、计算、输出结果。这里每个window都是分开计算的，所以第一个window里的sensor_1和第二个window里的sensor_1并没有累计
 **/
public class WindowTest2_TimeWindow {
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

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream = mapStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s;
                        long windowEnd = timeWindow.getEnd();
                        int count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

        resultStream.print();

        env.execute();
    }
}
