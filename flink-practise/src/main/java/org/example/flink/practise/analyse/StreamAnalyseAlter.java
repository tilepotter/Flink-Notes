package org.example.flink.practise.analyse;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.example.flink.bean.SensorReading;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName StreamAnalyseAlter
 * @Author wangyingkang
 * @Date 2021/7/27 11:23
 * @Version 1.0
 * @Description 测试flink进行流处理分析预警的应用：预先设定每个传感器的预警温度值（当达到或超过这个温度值时候，传感器应该发生预警），
 * 对Kafka消息队列中到来的每个传感器的温度值跟预先设定的温度值进行比对，大于等于预警值的将超出的温度值写入redis（传感器ID与时间戳值作为key，超出温度值作为value）
 **/
public class StreamAnalyseAlter implements Runnable {

    private StreamExecutionEnvironment env;

    /**
     * kafka consumer props
     */
    private final Properties props;

    /**
     * Kafka topicName
     */
    private final String topicName;

    /**
     * alter SensorReading temperature map
     */
    private final static HashMap<String, Double> ALTER_MAP = new HashMap<>();

    /**
     * jedis连接配置
     */
    private FlinkJedisPoolConfig jedisPoolConfig;

    public StreamAnalyseAlter(String bootstrapServer, String groupId, String topicName) {
        //将需要预警的数据写入内存
        ALTER_MAP.put("sensor_1", 50.5);
        ALTER_MAP.put("sensor_2", 200.8);
        ALTER_MAP.put("sensor_3", 200.1);
        ALTER_MAP.put("sensor_4", 231.7);
        ALTER_MAP.put("sensor_5", 254.1);
        ALTER_MAP.put("sensor_6", 251.9);
        ALTER_MAP.put("sensor_7", 300.5);
        ALTER_MAP.put("sensor_8", 300.9);
        ALTER_MAP.put("sensor_9", 300.1);
        ALTER_MAP.put("sensor_10", 300.9);
        //Kafka消费者配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.props = properties;
        this.topicName = topicName;
        this.jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hdp01")
                .setPort(6379)
                .build();
    }

    /**
     * 对到来的数据进行处理转换，并与每个传感器的预警温度值进行比对，若超出预警值则写入redis
     */
    @Override
    public void run() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);

        //数据源-Kafka
        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>(this.topicName, new SimpleStringSchema(), this.props));

        //将string转换为SensorReading
        DataStream<SensorReading> mapStream = inputStream.map(line -> JSON.parseObject(line, SensorReading.class));

        //对到来的数据跟每个传感器的温度值与预警数据进行比对，超出预警温度值则写入redis
        DataStream<SensorReading> filterStream = mapStream.filter((FilterFunction<SensorReading>) sensorReading ->
                sensorReading.getTemperature() >= ALTER_MAP.get(sensorReading.getId()));

        filterStream.print("alter=>");


        //将预警温度值写入redis
        filterStream.addSink(new RedisSink<>(this.jedisPoolConfig, new MyRedisMapper()));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义redis mapper操作redis
     */
    public static class MyRedisMapper implements RedisMapper<SensorReading> {
        // 定义保存数据到redis的命令，存成Hash表，hset sensor_temp <id,temperature>
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "alter_sensor");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getId() + "_" + data.getTimestamp();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.getTemperature().toString();
        }
    }
}
