package org.example.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.flink.bean.SensorReading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName SinkTest4_Jdbc
 * @Author wangyingkang
 * @Date 2021/6/22 11:17
 * @Version 1.0
 * @Description 官方目前没有专门针对MySQL的，我们自己实现就好了
 **/

public class SinkTest4_Jdbc {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("data/sensor.txt");

        //将读取的数据进行map转化成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        Connection connection = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;


        //open方法进行初始化连接操作
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false", "root", "admin");
            //insertStatement = connection.prepareStatement("insert into sensor_temp(id,temp) values (?,?)");
            //updateStatement = connection.prepareStatement("update sensor_temp set temp= ? where id =?");
            //根据mysql特性：主键有数据则进行更新操作，没有则进行插入操作
            insertStatement=connection.prepareStatement("insert into sensor_temp(id,temp) values(?,?) on duplicate key update temp=?");

        }

        // 每来一条数据，调用链接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            insertStatement.setString(1,value.getId());
            insertStatement.setDouble(2,value.getTemperature());
            insertStatement.setDouble(3,value.getTemperature());
            insertStatement.execute();
            /*updateStatement.setDouble(1, value.getTemperature());
            updateStatement.setString(2, value.getId());
            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1, value.getId());
                insertStatement.setDouble(2, value.getTemperature());
                insertStatement.execute();
            }*/
        }

        //释放资源
        @Override
        public void close() throws Exception {
            insertStatement.close();
            //updateStatement.close();
            connection.close();
        }
    }
}
