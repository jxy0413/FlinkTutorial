package cn.bjfu.testApi;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Testsform_RollingAggretion {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> inputStream = env.addSource(new MySensorSource());

        KeyedStream<SensorReading, Tuple> keyStream = inputStream.keyBy("id");
        SingleOutputStreamOperator<SensorReading> resultStream = keyStream.maxBy("temperatue");
        resultStream.print();

        env.execute();
    }
}
