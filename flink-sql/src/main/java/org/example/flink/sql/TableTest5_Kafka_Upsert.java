package org.example.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName TableTest5_Kafka_Retract
 * @Author wangyingkang
 * @Date 2022/1/20 16:02
 * @Version 1.0
 * @Description 使用 upsert-kafka connector，消费Kafka指定topic的数据，并进行简单聚合统计，将聚合结果以upsert模式输出到另外的Kafka topic
 **/
public class TableTest5_Kafka_Upsert {
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
                "   'connector.topic' = 'test',\n" +
                "   'connector.properties.group.id' = 'main-group',\n" +
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
                "    cnt BIGINT,\n" +
                "    avgTemp DOUBLE,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED \n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'sink_test',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false'\n" +
                ")";

        //执行输出端的建表语句
        tableEnv.executeSql(dropSql2);
        tableEnv.executeSql(kafkaTableDDL2);

        //简单聚合查询，以upsert模式将结果输出到表sensor_2,即输出到Kafka 的sink_test topic
        String insertSql = "INSERT INTO sensor_2 " +
                "SELECT id , count(id) as cnt , avg(temp)  as avgTemp FROM sensor GROUP BY id";


        tableEnv.executeSql(insertSql);

        //查询表sensor_2
        TableResult result = tableEnv.executeSql("select * from sensor_2");
        result.print();
    }
}
