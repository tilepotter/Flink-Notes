package org.example.flink.table_api;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName TableTest3_ComonApi
 * @Author wangyingkang
 * @Date 2021/10/19 14:38
 * @Version 1.0
 * @Description TableEnvironment是flink中集成Table API和SQL的核心概念,创建表的执行环境，需要将flink流处理的执行环境传入
 * 1、 Table API基于代表"表"的Table类，并提供一整套操作处理的方法API；这些方法会返回一个新的Table对象，表示对输入表应用转换操作的结果
 * <p>
 * 2、 有些关系型转换操作，可以由多个方法调用组成，构成链式调用结构
 **/
public class TableTest3_ComonApi {
    public static void main(String[] args) throws Exception {
        // 1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、设置并行度
        env.setParallelism(1);

        // 3、创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4、表的创建：连接外部文件系统，读取数据
        // 4.1读取文件
        String path = "data/sensor.txt";
        tableEnv.connect(new FileSystem().path(path))       //定义到文件系统的连接
                .withFormat(new Csv())                      //定义以csv格式进行数据格式化
                .withSchema(new Schema()                    //定义表结构
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");   //创建临时表

        //创建表inputTable
        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();

        //tableEnv.toAppendStream(inputTable, Row.class).print();

        // 5、查询转换
        // 5.1 Table API
        // 使用Table API 进行简单转换
        Table resultTable = inputTable.select($("id"), $("temp"))
                .filter($("id").isEqual("sensor_6"));

        // 使用Table API 进行聚合统计
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id"), $("id").count().as("count"), $("temp").avg().as("avgTemp"));


        //旧的Table API
        /*Table resultTable = inputTable.select(  "id , temp")
             .filter("id === 'sensor_6'");

        Table resultTable2 = inputTable.select("id , timestamp , temp")
                .where("id = 'sensor_1'");

        Table aggTable = inputTable.groupBy("id")
                .select("id ,id.count as count ,temp.avg as avgTemp");*/


        // 5.2 SQL
        Table sqlResultTable = tableEnv.sqlQuery("select id , temp from inputTable where id='sensor_1'");

        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt , avg(temp) as avgTemp  from inputTable group by id");


        //打印输出
        //tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        //tableEnv.toAppendStream(resultTable2, Row.class).print("resultTable2");
        /* 利用撤回流方式输出*/
        tableEnv.toRetractStream(aggTable, Row.class).print("aggTable");
        //tableEnv.toAppendStream(sqlResultTable, Row.class).print("sqlResultTable");
       // tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAggTable");  // 利用撤回流方式输出


        env.execute();
    }

}
