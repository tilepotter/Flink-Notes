package org.example.flink.practise.analyse;

/**
 * @author wangyingkang
 * @version 1.0
 * @date 2023/1/5 14:59
 * @Description
 */
public class StreamAnalyseAlterJob {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "hdp01:9092,hdp02:9092,hdp03:9092";
        String topicName = "sensor";
        String groupId = "client-group";

        StreamAnalyseAlter analyseAlter = new StreamAnalyseAlter(bootstrapServers, groupId, topicName);
        new Thread(analyseAlter).start();
    }
}
