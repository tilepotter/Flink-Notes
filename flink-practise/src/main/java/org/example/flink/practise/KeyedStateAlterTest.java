package org.example.flink.practise;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.example.flink.custom.MyKafkaConsumerProp;
import org.example.flink.bean.SensorReading;

/**
 * @author wangyingkang
 * @version 1.0
 * @date 2023/1/5 9:37
 * @Description 状态编程测试
 */
public class KeyedStateAlterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从Kafka消费数据
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(),MyKafkaConsumerProp.properties));

        //转为java bean
        DataStream<SensorReading> mapStream = source.map(line -> JSON.parseObject(line, SensorReading.class));

        //按传感器id分流后，计算每一个传感器近两次的温度差
        DataStream<Tuple3<String, Double, Double>> flatMapStream = mapStream.keyBy(SensorReading::getId)
                .flatMap(new AlterFlatMap(10.0));

        flatMapStream.print();

        env.execute("KeyedAlterTest");
    }

    public static class AlterFlatMap extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        /**
         * 温度差预警阈值
         */
        private Double threshold;

        /**
         * 中间状态
         */
        private ValueState<Double> valueState;

        public AlterFlatMap(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //从运行时上下文获取状态
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            //获取上一次存储的中间状态值
            Double lastTemp = valueState.value();

            //当前温度值
            Double currentTemp = sensorReading.getTemperature();

            if (lastTemp != null) {
                if (Math.abs(currentTemp - lastTemp) >= threshold) {
                    collector.collect(new Tuple3<>(sensorReading.getId(), currentTemp, lastTemp));
                }
            }

            //更新中间状态值为当前温度值
            valueState.update(currentTemp);
        }

        /**
         * 手动释放资源
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            valueState.clear();
        }
    }
}
