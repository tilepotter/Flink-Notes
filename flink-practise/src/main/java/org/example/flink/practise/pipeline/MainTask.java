package org.example.flink.practise.pipeline;

/**
 * @ClassName MainTask
 * @Author wangyingkang
 * @Date 2021/12/4 13:38
 * @Version 1.0
 * @Description
 **/
public class MainTask {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9092";
        String topicName = "sensor";
        String groupId = "client-group";
        StreamPipeline streamPipeline = new StreamPipeline(bootstrapServers, groupId, topicName);
        Thread thread = new Thread(streamPipeline);
        thread.start();
    }
}
