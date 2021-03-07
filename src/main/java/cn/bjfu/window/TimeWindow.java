package cn.bjfu.window;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());
        //开窗测试
        dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(10));
        env.execute();
    }
}
