package org.example.flink.practise.analyse;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.example.flink.bean.SensorReading;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @ClassName RandomDataToKafka
 * @Author wangyingkang
 * @Date 2021/7/27 10:40
 * @Version 1.0
 * @Description 传感器温度值随机生成程序，并把传感器随机数据发送到Kafka
 **/
public class RandomDataToKafka implements Runnable {

    private KafkaProducer<Long, String> kafkaProducer;

    private final String topicName;

    /**
     * 标志位，控制数据的产生
     */
    private volatile boolean flag = true;

    /**
     * 是否异步发送开关
     */
    private volatile boolean isAsync = false;

    /**
     * RandomDataToKafka Class Constructor
     *
     * @param brokerList kafka broker list
     * @param topicName  kafka topic name
     */
    public RandomDataToKafka(String brokerList, String topicName) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(props);
        this.topicName = topicName;
    }

    /**
     * 生成传感器随机数据，并把随机数据发送到Kafka
     */
    @Override
    public void run() {
        //定义一个随机数发生器
        Random random = new Random();

        // 设置10个传感器的初始温度
        HashMap<String, Double> sensorTempMap = new HashMap<>();
        for (int i = 0; i < 10; ++i) {
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

        while (flag) {
            for (String sensorId : sensorTempMap.keySet()) {
                // 在当前温度基础上随机波动
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                sensorTempMap.put(sensorId, newTemp);
                SensorReading sensorReading = new SensorReading(sensorId, System.currentTimeMillis(), newTemp);
                long startTime = System.currentTimeMillis();
                //发送消息的key
                long key = sensorReading.getTimestamp();
                //发送消息的value
                String messageStr = JSON.toJSONString(sensorReading);
                //异步发送
                if (isAsync) {
                    Future<RecordMetadata> send = this.kafkaProducer.send(new ProducerRecord<Long, String>(topicName, key, messageStr),
                            new ProducerCallback(startTime, key, messageStr));
                } else {
                    //同步发送
                    try {
                        this.kafkaProducer.send(new ProducerRecord<Long, String>(topicName, key, messageStr));
                        System.out.println("thread " + Thread.currentThread().getName() + " Sent message: (" + "key=" + key + ", " + "value=" + messageStr + ")");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // 控制输出频率
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        String brokerList = "hdp01:9092,hdp02:9092,hdp03:9092";
        String topicName = "sensor";
        //生产者数量
        int producerCount = 3;
        for (int i = 0; i < producerCount; i++) {
            RandomDataToKafka producer = new RandomDataToKafka(brokerList, topicName);
            new Thread(producer).start();
        }

    }
}

class ProducerCallback implements Callback {
    private final long startTime;
    private final long key;
    private final String message;

    public ProducerCallback(long startTime, long key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + "key=" + key + ", " + "value=" + message + ") send to partition(" + metadata.partition() + "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms"
            );
        } else {
            //处理发送异常的消息
            e.printStackTrace();
        }
    }
}
