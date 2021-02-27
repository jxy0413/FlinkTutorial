package cn.bjfu.testApi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest_File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        dataStream.print();
        env.execute();
    }
}
