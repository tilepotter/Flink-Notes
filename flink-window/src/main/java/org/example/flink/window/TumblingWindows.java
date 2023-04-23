package org.example.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @ClassName TumblingWindows
 * @Author wangyingkang
 * @Date 2021/7/13 14:27
 * @Version 1.0
 * @Description 滚动窗口：滚动窗口 (Tumbling Windows) 是指彼此之间没有重叠的窗口。例如：每隔1小时统计过去1小时内的商品点击量，那么 1 天就只能分为 24 个窗口，每个窗口彼此之间是不存在重叠的
 * 每隔三秒统计上个单词出现的次数
 **/
public class TumblingWindows {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        //env.setParallelism(1);

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

        //每隔3秒统计一次每个单词出现的数量
        flatMapStream.keyBy(0).timeWindow(Time.seconds(3)).sum(1).print();

        env.execute("tumblingWindows");
    }
}
