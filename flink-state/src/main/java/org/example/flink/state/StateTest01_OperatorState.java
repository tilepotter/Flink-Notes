package org.example.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.bean.SensorReading;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName StateTest01_OperatorState
 * @Author wangyingkang
 * @Date 2021/8/23 09:50
 * @Version 1.0
 * @Description OperatorState算子状态测试代码
 **/
public class StateTest01_OperatorState {
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
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());

        resultStream.print();

        env.execute();
    }

    //自定义MapFunction
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        //定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> list) throws Exception {
            for (Integer integer : list) {
                count += integer;
            }
        }
    }
}
