package org.example.flink.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @ClassName HBaseUtil
 * @Author wangyingkang
 * @Date 2021/7/27 17:14
 * @Version 1.0
 * @Description
 **/
public class HBaseUtil {
    /**
     * @param zkQuorum zookeeper地址，多个要用逗号分隔
     * @param port     zookeeper端口号
     * @return connection
     */
    public static Connection getConnection(String zkQuorum, int port) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", port + "");

        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }
}
