package org.example.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @ClassName SlidingWindows
 * @Author wangyingkang
 * @Date 2021/7/13 15:27
 * @Version 1.0
 * @Description 滑动窗口用于滚动进行聚合分析，例如：每隔 6 分钟统计一次过去一小时内所有商品的点击量，那么统计窗口彼此之间就是存在重叠的，即 1天可以分为 240 个窗口。
 **/
public class SlidingWindows {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //设置streamTime格式，否则程序报错：Caused by: java.lang.RuntimeException: Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //接收socket上的数据输入
        DataStream<String> streamSource = env.socketTextStream("localhost", 9999, "\n", 3);


        //将接收到的数据按'\t'进行切割，并收集进二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] fields = value.split("\t");
                for (String field : fields) {
                    collector.collect(new Tuple2<>(field, 1L));
                }
            }
        });

        //滑动窗口：每隔3秒统计过去一分钟的数据
        //flatMapStream.keyBy(0).timeWindow(Time.minutes(1),Time.seconds(10)).sum(1).print();

        // GlobalWindows 全局窗口：当单词累计出现的次数每达到10次时，则触发计算，计算整个窗口内该单词出现的总数
        flatMapStream.keyBy(0).window(GlobalWindows.create()).trigger(CountTrigger.of(5)).sum(1).print();

        env.execute("tumblingWindows");
    }
}
