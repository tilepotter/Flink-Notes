package org.example.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName EventTimeSQL
 * @Author wangyingkang
 * @Date 2021/12/29 10:00
 * @Version 1.0
 * @Description
 **/
public class EventTimeSQL {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //基于Blink的流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //获取文件路径
        String filePath = EventTimeSQL.class.getClassLoader().getResource("orders.csv").getPath();

        System.out.println(filePath);
        //orders表ddl
        String ddl =
                "create table orders (\n" +
                        " user_id INT ,\n" +
                        " product STRING , \n" +
                        " amount INT , \n" +
                        " ts TIMESTAMP(3) ,\n" +
                        " WATERMARK FOR ts as ts - INTERVAL '3' SECOND \n" +
                        ") WITH (\n" +
                        " 'connector.type' = 'filesystem' , \n" +
                        " 'connector.path' = '" + filePath + "' ,\n" +
                        " 'format.type' = 'csv' \n" +
                        ")";

        //执行建表语句
        tableEnv.executeSql(ddl);

        //聚合查询sql
        String sql =
                " SELECT TUMBLE_START(ts,INTERVAL '3' SECOND) ," +
                        " COUNT(DISTINCT product), \n" +
                        " SUM(amount) total_nums \n" +
                        " COUNT(*) orders_nums \n" +
                        " FROM orders \n" +
                        " GROUP BY TUMBLE(ts, INTERVAL '3' SECOND)";

        //执行聚合查询语句
        Table table = tableEnv.sqlQuery(sql);

        //聚合查询结果转换为二元组的撤回流
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);

        tuple2DataStream.print();

        env.execute();

    }
}
