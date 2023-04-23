package org.example.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName WindowTest3_CountWindow
 * @Author wangyingkang
 * @Date 2021/7/19 14:05
 * @Version 1.0
 * @Description 测试滑动计数窗口的增量聚合函数
 * 滑动窗口，当窗口不足设置的大小时，会先按照步长输出。
 * eg：窗口大小10，步长2，那么前5次输出时，窗口内的元素个数分别是（2，4，6，8，10），再往后就是10个为一个窗口了。
 **/
public class WindowTest3_CountWindow {
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

        SingleOutputStreamOperator<Double> resultStream = mapStream.keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new MyAvgFunction());

        resultStream.print();

        env.execute();
    }

    public static class MyAvgFunction implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            // 温度累加求和，当前统计的温度个数+1
            return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}