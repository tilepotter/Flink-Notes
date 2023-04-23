package org.example.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: wyk
 * @date 2021/5/26 16:48
 * @Description 测试Flink从文件中读取数据
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1，使得任务抢占同一线程
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> fileStream = env.readTextFile("data/sensor.txt");

        //打印输出
        fileStream.print();

        //执行程序
        env.execute("FileSource");
    }
}
