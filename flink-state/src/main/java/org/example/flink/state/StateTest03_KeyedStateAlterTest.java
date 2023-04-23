package org.example.flink.state;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.example.flink.bean.SensorReading;

import java.util.Properties;

/**
 * @ClassName StateTest03_KeyedStateAlterTest
 * @Author wangyingkang
 * @Date 2021/8/23 13:55
 * @Version 1.0
 * @Description 温度报警测试：如果一个传感器前后温差超过10度就报警。这里使用键控状态Keyed State + flatMap来实现
 **/
public class StateTest03_KeyedStateAlterTest implements Runnable {

    //kafka consumer props
    private final Properties props;

    //Kafka topicName
    private final String topicName;

    public StateTest03_KeyedStateAlterTest(String topicName, String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.props = properties;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //从socket文本流中读取数据
        //DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //从Kafka消费数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props));

        //转换为SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> JSON.parseObject(line, SensorReading.class));

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy(SensorReading::getId)
                .flatMap(new MyFlatMap(10.0));

        resultStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyFlatMap extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        //报警的温度阈值
        private final Double threshold;

        //键控状态，记录上一次的温度值
        ValueState<Double> lastTemperature;

        public MyFlatMap(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //从运行时上下文中获取状态
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double lastTemp = lastTemperature.value();
            Double curTemp = sensorReading.getTemperature();

            // 如果不为空，判断是否温差超过阈值，超过则报警
            if (lastTemp != null) {
                if (Math.abs(curTemp - lastTemp) >= threshold) {
                    collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp, curTemp));
                }
            }

            // 更新保存的"上一次温度"
            lastTemperature.update(curTemp);
        }

        @Override
        public void close() throws Exception {
            //手动释放资源
            lastTemperature.clear();
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = "hdp01:9092,hdp02:9092,hdp03:9092";
        String topicName = "sensor";
        String groupId = "client-group";
        StateTest03_KeyedStateAlterTest test = new StateTest03_KeyedStateAlterTest(topicName, bootstrapServers, groupId);
        new Thread(test).start();
    }
}
