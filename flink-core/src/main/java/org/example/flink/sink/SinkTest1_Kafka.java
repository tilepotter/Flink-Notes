package org.example.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.example.flink.bean.SensorReading;

import java.util.Properties;

/**
 * @ClassName SinkTest1_Kafka
 * @Author wangyingkang
 * @Date 2021/6/21 17:01
 * @Version 1.0
 * @Description 官方提供了一部分的框架的sink。除此以外，需要用户自定义实现sink。
 *              此例中的KafkaSink相当于一个pipelined，消费一个topic的数据并进行etl转换后写入到另外一个topic，实现了数据管道的功能。
 **/
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        Properties properties = new Properties();
        //properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.50:9092");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-01");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //从Kafka中读取数据
        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        //序列化从Kafka中读取的数据
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        //将数据写入Kafka
        dataStream.addSink(new FlinkKafkaProducer<>("127.0.0.1:9092", "sinktest", new SimpleStringSchema()));

        //dataStream.print();

        env.execute();

    }
}
