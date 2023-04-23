package org.example.flink.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName TableTest_FileOutPut
 * @Author wangyingkang
 * @Date 2021/10/19 16:55
 * @Version 1.0
 * @Description sink端使用Table API 的  Connector Tables连接外部文件系统
 * 数据写入到文件测试：写入到文件有局限，只能是批处理，且只能是追加写，不能是更新式的随机写。
 **/
public class TableTest4_FileOutput_TableAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 1.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.表的创建：连接外部系统，读取数据
        String inputPath = "data/sensor.txt";
        tableEnv.connect(new FileSystem().path(inputPath))       //定义到文件系统的连接
                .withFormat(new Csv())                      //定义以csv格式ji进行数据格式化
                .withSchema(new Schema()                    //定义表结构
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");   //创建临时表

        Table inputTable = tableEnv.from("inputTable");

        // 3.查询id=sensor_1的数据
        Table resultTable = inputTable.select($("id"), $("temp"))
                .filter($("id").isEqual("sensor_1"));

        // 4.输出到文件
        // 连接外部文件注册输出表
        String outputPath = "data/output.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        tableEnv.execute("");
    }

}
