package org.example.flink.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @ClassName TableTest5_KafkaPipeLine
 * @Author wangyingkang
 * @Date 2021/12/6 16:48
 * @Version 1.0
 * @Description 使用Table API连接Kafka：消费Kafka数据，按条件查出数据以后写入Kafka不同topic
 **/
public class TableTest5_Kafka_TableAPI {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.连接Kafka，消费sensor topic的数据
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092")
                ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");

        //4.查询转换
        Table inputTable = tableEnv.from("inputTable");

        //新版Table API
        Table resultTable = inputTable.select($("id"), $("timestamp"), $("temp"))
                .filter($("id").isEqual("sensor_6"));

        //聚合统计
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id").count().as("count"), $("temp").avg().as("avgTemp"));

        /*旧版Table API
        Table resultTable = inputTable.select("id, timestamp, temp")
                .filter("id === 'sensor_6'");

        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count ,temp.avg as avgTemp");*/

        //6.建立Kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("sinktest")
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092")
                ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        //查询结果集输出到outputTable
        resultTable.insertInto("outputTable");

        tableEnv.execute("");
    }
}
