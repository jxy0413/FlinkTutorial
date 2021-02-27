package cn.bjfu.day01;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

//        String inputPath = "C:\\Users\\Administrator\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt" ;
////        DataStreamSource<String> inputStream = env.readTextFile(inputPath);
        //用Flink parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputStream = env.socketTextStream(host,port).slotSharingGroup("red");

        inputStream.flatMap(new WordCount.MyflatMapper()).keyBy(0).sum(1).print();

        env.execute();
    }
}
