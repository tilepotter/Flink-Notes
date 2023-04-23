package org.example.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName TableTest5_KafkaPipeLine_SQL
 * @Author wangyingkang
 * @Date 2022/1/20 15:03
 * @Version 1.0
 * @Description 使用Flink SQL读写Kafka:消费Kafka数据，按条件查出数据以后写入Kafka不同topic
 **/
public class TableTest5_Kafka_SQL {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.连接Kafka，消费sensor topic的数据
        String inputTable = "sensor";
        String dropSql = "DROP TABLE IF EXISTS " + inputTable;
        String kafkaTableDDL
                = "CREATE TABLE " + inputTable + " (\n" +
                "    id String,\n" +
                "    ts BIGINT,\n" +
                "    temp DOUBLE\n" +
                ") WITH (\n" +
                "   'connector.type' = 'kafka',\n" +
                "   'connector.version' = 'universal',\n" +
                "   'connector.topic' = 'sensor',\n" +
                "   'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "   'format.type' = 'csv',\n" +
                "   'update-mode' = 'append'\n" +
                ")";

        //执行输入端的建表语句
        tableEnv.executeSql(dropSql);
        tableEnv.executeSql(kafkaTableDDL);

        //6.建立Kafka连接，输出到不同的topic下
        String outputTable = "sensor_2";
        String dropSql2 = "DROP TABLE IF EXISTS " + outputTable;
        String kafkaTableDDL2
                = "CREATE TABLE " + outputTable + " (\n" +
                "    id String,\n" +
                "    ts BIGINT,\n" +
                "    temp DOUBLE\n" +
                ") WITH (\n" +
                "   'connector.type' = 'kafka',\n" +
                "   'connector.version' = 'universal',\n" +
                "   'connector.topic' = 'sinktest',\n" +
                "   'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "   'format.type' = 'csv',\n" +
                "   'update-mode' = 'append'\n" +
                ")";

        //执行输出端的建表语句
        tableEnv.executeSql(dropSql2);
        tableEnv.executeSql(kafkaTableDDL2);

        //简单查询转换，将结果输出到表sensor_2,即输出到Kafka 的sinktest topic
        String insertSql = "INSERT INTO sensor_2 " +
                "SELECT id , ts , temp FROM sensor WHERE id='sensor_6'";

        tableEnv.executeSql(insertSql);
    }
}
