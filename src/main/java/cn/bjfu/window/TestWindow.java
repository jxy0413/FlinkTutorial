package cn.bjfu.window;

import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> txtStream = env.readTextFile("");
        txtStream.map(new MapFunction<String, HelloSource>() {

            public HelloSource map(String value) throws Exception {
                String[] split = value.split(",");
                HelloSource helloSource = new HelloSource();
                helloSource.setEvent(split[0]);
                helloSource.setSource(split[1]);
                helloSource.setEvent(split[2]);
                return helloSource;
            }
        });
        txtStream.keyBy("source")
                .timeWindow(Time.minutes(5))
                .aggregate(new TestProcess())

    }
}

class TestProcess implements AggregateFunction<HelloSource,Integer,Integer> {
    public Integer createAccumulator() {
        return 0;
    }

    public Integer add(HelloSource value, Integer accumulator) {
        return accumulator+1;
    }

    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    public Integer merge(Integer a, Integer b) {
        return a+b;
    }
}
@Data
class HelloSource{
    private Long eventTime;
    private String source;
    private String event;
}