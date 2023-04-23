package org.example.flink.state;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName StateTest04_StateBackend
 * @Author wangyingkang
 * @Date 2021/8/30 09:48
 * @Version 1.0
 * @Description 状态后端测试
 * 1、MemoryStateBackend
 * 内存级的状态后端，会将键控状态作为内存中的对象进行管理，将它们存储在TaskManager的JVM堆上，而将checkpoint存储在JobManager的内存中
 * 特点：快速、低延迟，但不稳定
 * 2、FsStateBackend（默认）
 * 将checkpoint存到远程的持久化文件系统（FileSystem）上，而对于本地状态，跟MemoryStateBackend一样，也会存在TaskManager的JVM堆上
 * 同时拥有内存级的本地访问速度，和更好的容错保证
 * 3、RocksDBStateBackend
 * 将所有状态序列化后，存入本地的RocksDB中存储
 **/
public class StateTest04_StateBackend {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换为SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();

        env.execute();
    }
}
