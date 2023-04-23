package org.example.flink.sink;

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

import java.util.Properties;

/**
 * @ClassName SinkTest2_Redis
 * @Author wangyingkang
 * @Date 2021/6/21 18:19
 * @Version 1.0
 * @Description 此例消费Kafka指定topic中的数据，并将数据转换为指定键值对，使用hset写到redis中。
 **/
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        Properties properties = new Properties();
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

        //将从Kafka中读取的数据进行map转化成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义jedis连接配置
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();

        //将数据写到redis中
        dataStream.addSink(new RedisSink<>(jedisPoolConfig, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<SensorReading> {
        // 定义保存数据到redis的命令，存成Hash表，hset sensor_temp <id,temperature>
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_tmp");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.getTemperature().toString();
        }


    }
}
