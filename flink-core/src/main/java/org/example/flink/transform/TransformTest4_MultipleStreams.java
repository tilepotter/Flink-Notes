package org.example.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @author: wyk
 * @date 2021/6/11 10:55
 * @Description 1。DataStream -> SplitStream -> DataStream：DataStream通过指定条件切割成SplitStream，
 * 从一个SplitStream中获取一个或者多个DataStream（split&select操作）;我们可以结合split&select将一个DataStream拆分成多个DataStream。
 * <p>
 * 2。测试场景：根据传感器温度高低，划分成两组，high和low（>30归入high）：
 * 这个我发现在Flink当前时间最新版1.12.1已经不是DataStream的方法了，被去除了
 * 以下代码针对 Flink1.10.1
 * <p>
 * 3。union和connect的区别
 * Connect 的数据类型可以不同，Connect 只能合并两个流；
 * Union可以合并多条流，Union的数据结构必须是一样的；
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从text种读取数据
        DataStreamSource<String> dataStream = env.readTextFile("data/sensor.txt");

        //4.map操作
        DataStream<SensorReading> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //5.分流 -> 按照温度值30度为界分为两条流
        /*SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempStream.print("all");
        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);

        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        // 3. union联合多条流
        //warningStream.union(lowTempStream); 这个不行，因为warningStream类型是DataStream<Tuple2<String, Double>>，而highTempStream是DataStream<SensorReading>
        highTempStream.union(lowTempStream, allTempStream);
        resultStream.print();*/
        env.execute();
    }
}
