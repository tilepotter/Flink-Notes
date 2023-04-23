package org.example.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName TableTest4_FileOutput_SQL
 * @Author wangyingkang
 * @Date 2022/1/20 11:33
 * @Version 1.0
 * @Description sink端使用SQL DDL连接外部文件系统
 **/
public class TableTest4_FileOutput_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 1.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.表的创建：连接外部系统，读取数据
        String inputPath = "data/sensor.txt";
        String inputTableDDL =
                "create table inputTable (\n" +
                        " id STRING,\n" +
                        " ts BIGINT, \n" +
                        " temp DOUBLE\n" +
                        ") WITH (\n" +
                        " 'connector.type' = 'filesystem',\n" +
                        " 'connector.path' = '" + inputPath + "',\n" +
                        " 'format.type' = 'csv'\n" +
                        ")";

        //执行source建表语句
        tableEnv.executeSql(inputTableDDL);


        // 3.输出到文件
        // 连接外部文件注册输出表
        String outputPath = "data/outputTable.txt";
        //创建输出到外部文件系统的outputTable表
        String outputTableDDL =
                "create table outputTable (\n" +
                        " id STRING,\n" +
                        " temp DOUBLE\n" +
                        ") WITH (\n" +
                        " 'connector.type' = 'filesystem',\n" +
                        " 'connector.path' = '" + outputPath + "',\n" +
                        " 'format.type' = 'csv'\n" +
                        ")";
        //执行sink建表语句
        tableEnv.executeSql(outputTableDDL);

        // 将表inputTable的查询结果输出到outputTable
        tableEnv.executeSql(
                "INSERT INTO outputTable " +
                        "SELECT id , temp FROM inputTable WHERE id='sensor_1' ");

        tableEnv.execute("kafka-table-api");
    }
}
