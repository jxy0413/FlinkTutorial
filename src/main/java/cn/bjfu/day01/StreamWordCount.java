package cn.bjfu.day01;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "C:\\Users\\Administrator\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt" ;
        DataStreamSource<String> inputStream = env.readTextFile(inputPath);

        inputStream.flatMap(new WordCount.MyflatMapper()).keyBy(0).sum(1).print();

        env.execute();
    }
}
