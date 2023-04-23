package org.example.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @author: wyk
 * @date 2021/5/28 10:01
 * @Description 测试flink从自定义数据源中读取数据
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置执行环境
        env.setParallelism(1);

        //3.从自定义数据源读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }
}
