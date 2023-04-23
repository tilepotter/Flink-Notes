package org.example.flink.practise.pipeline;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.example.flink.bean.SensorReading;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName StreamPipeline
 * @Author wangyingkang
 * @Date 2021/7/27 16:54
 * @Version 1.0
 * @Description flink数据管道测试：消费Kafka中的数据，写入Hbase
 **/
public class StreamPipeline implements Runnable {

    /**
     * kafka consumer props
     */
    private final Properties props;

    /**
     * Kafka topicName
     */
    private final String topicName;

    /**
     * StreamPipeline Constructor
     *
     * @param bootstrapServer kafka broker servers
     * @param groupId         kafka consumer group id
     * @param topicName       kafka topic name
     */
    public StreamPipeline(String bootstrapServer, String groupId, String topicName) {
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
    }

    @Override
    public void run() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //设置Kafka source
        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), props));

        //map 转换
        DataStream<SensorReading> mapStream = inputStream.map(line -> JSON.parseObject(line, SensorReading.class));

        mapStream.addSink(new MyHbaseSink());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyHbaseSink extends RichSinkFunction<SensorReading> {
        private Integer maxSize = 1000;

        private Long delayTime = 5000L;

        public MyHbaseSink() {
        }

        public MyHbaseSink(Integer maxSize, Long delayTime) {
            this.maxSize = maxSize;
            this.delayTime = delayTime;
        }

        private Connection connection;
        private Long lastInvokeTime;
        private List<Put> puts = new ArrayList<>(maxSize);

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 获取全局配置文件，并转为ParameterTool
            //ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

            //创建一个Hbase的连接
            /*connection = HBaseUtil.getConnection(
                    params.getRequired("hbase.zookeeper.quorum"),
                    params.getInt("hbase.zookeeper.property.clientPort"));*/
            connection = HBaseUtil.getConnection(
                    "localhost",
                    2181);

            //获取系统当前时间
            lastInvokeTime = System.currentTimeMillis();
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            String rowKey = value.getId();

            //创建put对象，并赋rowkey值
            Put put = new Put(rowKey.getBytes());
            //添加列和列族
            put.addColumn("sensor".getBytes(), value.getTimestamp().toString().getBytes(), value.getTemperature().toString().getBytes(StandardCharsets.UTF_8));
            // 添加put对象到list集合
            puts.add(put);
            //使用ProcessingTime
            long currentTime = System.currentTimeMillis();
            //开始批次提交数据
            if (puts.size() == maxSize || currentTime - lastInvokeTime >= delayTime) {
                //获取一个Hbase表
                Table table = connection.getTable(TableName.valueOf("sensor_table"));
                table.put(puts);//批次提交
                puts.clear();
                lastInvokeTime = currentTime;
                table.close();
            }

        }

        @Override
        public void close() throws Exception {
            connection.close();
        }

    }
}
