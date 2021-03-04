package cn.bjfu.testApi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transform {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\Administrator\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {

            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        SingleOutputStreamOperator<String> flatMap = inputStream.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                return value.startsWith("1");
            }
        });

        mapStream.print("map");
        flatMap.print("flatMap");
        filterStream.print("filter");
        env.execute();
    }
}
