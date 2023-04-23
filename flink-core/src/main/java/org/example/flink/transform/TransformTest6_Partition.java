package org.example.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName TransformTest6_Partition
 * @Author wangyingkang
 * @Date 2021/6/21 16:41
 * @Version 1.0
 * @Description 分区操作，在DataStream类中可以看到很多Partitioner字眼的类。
 *              其中partitionCustom(...)方法用于自定义重分区
 **/
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(4);

        //3.从textFile读取数据
        DataStreamSource<String> inputStream = env.readTextFile("data/sensor.txt");

        //4.转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // SingleOutputStreamOperator多并行度默认就rebalance,轮询方式分配
        dataStream.print();

        // 1. shuffle (并非批处理中的获取一批后才打乱，这里每次获取到直接打乱且分区)
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        // 2. keyBy (Hash，然后取模)
        dataStream.keyBy(SensorReading::getId).print("keyBy");

        // 3. global (直接发送给第一个分区，少数特殊情况才用)
        dataStream.global().print("global");

        env.execute();
    }
}
