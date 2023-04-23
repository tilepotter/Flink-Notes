package org.example.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.flink.bean.SensorReading;

import java.util.HashMap;
import java.util.Random;

/**
 * @author: wyk
 * @date 2021/5/28 10:08
 * @Description 自定义数据源
 */
public class MySensorSource implements SourceFunction<SensorReading> {

    //标志位，控制数据的产生
    private volatile boolean flag = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        //定义一个随机数发生器
        Random random = new Random();

        // 设置10个传感器的初始温度
        HashMap<String, Double> sensorTempMap = new HashMap<>();
        for (int i = 0; i < 10; ++i) {
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

        while (flag) {
            for (String sensorId : sensorTempMap.keySet()) {
                // 在当前温度基础上随机波动
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                sensorTempMap.put(sensorId, newTemp);
                sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
            }
            // 控制输出评率
            Thread.sleep(2000L);
        }
    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}
