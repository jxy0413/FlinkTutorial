package cn.bjfu.testApi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

public class SourceTest1  {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("1", 15454949L, 131.2),
                new SensorReading("2", 15454949L, 131.2),
                new SensorReading("3", 15454949L, 131.2)
        ));
        dataStream.print("data");
        env.execute();
    }
}
