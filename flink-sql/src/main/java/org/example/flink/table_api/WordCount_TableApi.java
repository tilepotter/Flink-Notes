package org.example.flink.table_api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.example.flink.bean.WordCount;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName WordCount_TableApi
 * @Author wangyingkang
 * @Date 2022/1/18 10:38
 * @Version 1.0
 * @Description 使用TableApi和BatchTable实现单词计频
 **/
public class WordCount_TableApi {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //创建表环境BatchTableEnv
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(env);

        DataSet<WordCount> wcDataSet = env.fromElements(
                new WordCount("java", 1),
                new WordCount("hadoop", 1),
                new WordCount("hive", 1),
                new WordCount("spark", 1),
                new WordCount("flink", 1),
                new WordCount("java", 1)
        );

        //将DataSet转换为Table
        Table table = batchTableEnv.fromDataSet(wcDataSet);

        //对table进行分组查询：按单词名进行分组进行频次统计，且频次大于1
        Table resultTable = table.groupBy($("wordName"))
                .select($("wordName"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isGreater(1));

        //打印表结构
        resultTable.printSchema();

        //将table转换为DataSet
        DataSet<WordCount> dataSet = batchTableEnv.toDataSet(resultTable, WordCount.class);
        //打印结果
        dataSet.print();
    }
}
