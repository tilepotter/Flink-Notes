package org.example.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName TableTest7_JDBC
 * @Author wangyingkang
 * @Date 2021/12/28 09:25
 * @Version 1.0
 * @Description source端和sink端都使用SQL来进行操作：消费kafka指定topic数据，并进行简单聚合查询，以upsert模式 'a将聚合统计结果输出到mysql
 **/
public class TableTest7_JDBC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.连接Kafka，读取数据
        String kafkaTableDDL
                = "CREATE TABLE  inputTable (\n" +
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
        tableEnv.executeSql(kafkaTableDDL);

        //4.sink端连接mysql，将数据进行输出
        String sinkDDL =
                "create table sensor_count (" +
                        " id varchar(20) not null, " +
                        " cnt bigint not null, " +
                        " avg_temp double not null " +
                        ") with (" +
                        " 'connector.type' = 'jdbc', " +
                        " 'connector.url' = 'jdbc:mysql://localhost:3306/test', " +
                        " 'connector.table' = 'sensor_count', " +
                        " 'connector.driver' = 'com.mysql.cj.jdbc.Driver', " +
                        " 'connector.username' = 'root', " +
                        " 'connector.password' = 'admin', " +
                        " 'connector.write.flush.interval' = '2s' )";

        //执行ddl创建表
        tableEnv.executeSql(sinkDDL);

        //5.聚合数据插入jdbc表
        String insertSql = "INSERT INTO sensor_count " +
                "SELECT id, count(id) as cnt, avg(temp) as avgTemp FROM inputTable GROUP BY id";

        tableEnv.executeSql(insertSql);
    }
}
