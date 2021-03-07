package cn.bjfu.testApi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Partition {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        dataStream.print("input");

        //1、shuffle
        DataStream<String> rebalanceStream = dataStream.rebalance();
        rebalanceStream.print("rebalanceStream");

        env.execute();
    }
}
