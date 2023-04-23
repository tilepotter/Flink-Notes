package org.example.flink.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.example.flink.bean.SensorReading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName Sinktest3_Es
 * @Author wangyingkang
 * @Date 2021/6/22 10:24
 * @Version 1.0
 * @Description 此例消费Kafka指定topic中的数据，并将数据写到es中，不同es版本依赖不同，具体参考flink官方
 *              https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/connectors/elasticsearch.html
 **/
public class SinkTest3_Es {
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

        //定义es的连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));

        dataStream.addSink(new ElasticsearchSink.Builder(httpHosts,new MyEsSinkFunction()).build());

        env.execute();
    }

    //实现自定义的es写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{
        @Override
        public void process(SensorReading element, RuntimeContext context, RequestIndexer indexer) {
            //定义写入数据的source
            HashMap<String, String> map = new HashMap<>();
            map.put("id",element.getId());
            map.put("timestamp",element.getTimestamp().toString());
            map.put("temp",element.getTemperature().toString());

            //创建请求，作为向es发起的写入命令(ES7统一type就是_doc，不再允许指定type)
            IndexRequest request = Requests.indexRequest()
                    .index("sensor_tmp")
                    .type("sensor")
                    .source(map);

            // 用index发送请求
            indexer.add(request);

        }
    }
}

