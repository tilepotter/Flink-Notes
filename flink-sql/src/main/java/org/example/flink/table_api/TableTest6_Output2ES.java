package org.example.flink.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName TableTest6_Output2Es
 * @Author wangyingkang
 * @Date 2021/12/27 15:24
 * @Version 1.0
 * @Description 消费Kafka中的数据，聚合统计以后写到es
 **/
public class TableTest6_Output2ES {
    public static void main(String[] args) throws Exception {
        // 1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、设置并行度
        env.setParallelism(1);

        // 3、创建表执行环境
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        //4.连接Kafka，消费数据
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

        //5.聚合统计
        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();

        //新版Table API
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id").count().as("count"), $("temp").avg().as("avgTemp"));

        //旧版Table API
        // Table aggTable = inputTable.groupBy("id").select("id ,id.count as count, temp.avg as avgTemp");

        //6.连接es,聚合统计结果输出到es
        tableEnv.connect(new Elasticsearch()
                        .version("6")      //es版本
                        .host("127.0.0.1", 9200, "http")      //连接地址
                        .index("sensor")        //索引名
                        .documentType("temp")   //文档类型
                        .bulkFlushInterval(2000)  //批量插入到es的时间间隔
                )
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT())
                        .field("avgTemp", DataTypes.DOUBLE()))
                .createTemporaryTable("esOutputTable");

        //聚合结果输出到esOutputTable
        aggTable.insertInto("esOutputTable");

        tableEnv.execute("outPut2ES");
    }
}
