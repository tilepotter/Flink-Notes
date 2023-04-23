package org.example.flink.table_api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName TableTest2_CommonApi
 * @Author wangyingkang
 * @Date 2021/10/19 10:36
 * @Version 1.0
 * @Description Table API和SQL的程序结构，与流式处理的程序结构十分类似
 * StreamTableEnvironment tableEnv=...//创建表执行环境
 * <p>
 * //创建一张表，用于读取数据
 * tableEnv.connect(...).createTemporaryTable("inputTable");
 * <p>
 * //注册一张表，用于把计算结果输出
 * tableEnv.connect(...).createTemporaryTable("outputTable");
 * <p>
 * //通过Table API 查询算子，得到一张结果表
 * Table result = tableEnv.from("inputTable").select("...");
 * <p>
 * //通过sql查询语句，得到一张结果表
 * Table sqlResult = tableEnv.sqlQuery("select ... from inputTable");
 * <p>
 * //将结果表写入输出表中
 * result.insertInto("outputTable");
 **/
public class TableTest2_CommonEnv {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1
        env.setParallelism(1);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        // 1.2 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 1.3 基于新版Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 1.4 基于新版Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        StreamTableEnvironment blinkBatchTableEnv = StreamTableEnvironment.create(env, blinkBatchSettings);
    }
}