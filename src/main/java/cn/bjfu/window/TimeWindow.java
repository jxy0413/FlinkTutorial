package cn.bjfu.window;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());
        //增量开窗测试
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(10))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    public Integer createAccumulator() {
                        return 0;
                    }

                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        //全窗户函数
        SingleOutputStreamOperator<Object> singleStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, Object, Tuple, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    public void apply(Tuple tuple, org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Iterable<SensorReading> input, Collector<Object> out) throws Exception {
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect("窗口开始"+window.getStart()+":"+count+"窗口结束"+window.getEnd());
                    }
                });
        singleStream.print();
        env.execute();
    }
}
