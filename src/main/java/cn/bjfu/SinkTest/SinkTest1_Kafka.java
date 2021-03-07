package cn.bjfu.SinkTest;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class SinkTest1_Kafka {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        SingleOutputStreamOperator<String> dataStreamString = dataStream.map(new MapFunction<SensorReading, String>() {

            public String map(SensorReading value) throws Exception {
                return value.toString();
            }
        });

        dataStreamString.addSink(new FlinkKafkaProducer011<String>("Master:9092","sinkTest",new SimpleStringSchema()));

        env.execute();
    }
}
