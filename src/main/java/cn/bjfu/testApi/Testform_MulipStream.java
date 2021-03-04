package cn.bjfu.testApi;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class Testform_MulipStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> inputStream = env.addSource(new MySensorSource());

        SplitStream<SensorReading> splitStream = inputStream.split(new OutputSelector<SensorReading>() {

            public Iterable<String> select(SensorReading value) {
                return value.getTemperatue() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        highStream.print("highStream");


        DataStream<SensorReading> lowStream = splitStream.select("low");
        lowStream.print("lowStream");

        env.execute();
    }
}
