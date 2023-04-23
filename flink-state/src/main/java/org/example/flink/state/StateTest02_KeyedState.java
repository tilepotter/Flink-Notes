package org.example.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

/**
 * @ClassName StateTest02_KeyedState
 * @Author wangyingkang
 * @Date 2021/8/23 11:19
 * @Version 1.0
 * @Description KeyedState键控状态测试代码
 **/
public class StateTest02_KeyedState {
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

        //定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                .map(new MyMapper());

        resultStream.print("result");

        env.execute();
    }

    //自定义map富函数，测试键控状态
    public static class MyMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> valueState;

        // 其它类型状态的声明
        /*private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;*/


        //通过初始化方法，环境上下午得到键控状态
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));
           /* myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-listState", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
            myReducingState=getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce",SensorReading.class));*/
        }

        //统计每个传感器的信息数量
        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其它状态API调用
            //listState
            /*for (String str : myListState.get()) {
                System.out.println(str);
            }
            myListState.add("hello");
            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");
            // reducing state
            // myReducingState.add(value);

            myMapState.clear();*/

            Integer count = valueState.value();
            // 第一次获取是null，需要判断
            count = count == null ? 0 : count;
            ++count;
            //更新状态
            valueState.update(count);
            return count;
        }

    }
}
