package cn.bjfu.testApi;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SourceKafkaTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","Master:9092");
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),prop));

        dataStream.print();
        env.execute();
    }
}
