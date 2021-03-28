package cn.bjfu.window;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        //增量聚合
//        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(5))
//                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                    public Integer createAccumulator() {
//                        return 0;
//                    }
//
//                    public Integer add(SensorReading value, Integer accumulator) {
//                        return accumulator + 1;
//                    }
//
//                    public Integer getResult(Integer accumulator) {
//                        return accumulator;
//                    }
//
//                    public Integer merge(Integer a, Integer b) {
//                        return a + b;
//                    }
//                });
//        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> resultStream = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(5))
//                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
//                        String field = tuple.getField(0);
//                        Long   windowEnd = window.getEnd();
//                        int count = IteratorUtils.toList(input.iterator()).size();
//                        out.collect(new Tuple3<String, Long, Integer>(field,windowEnd,count));
//                    }
//                });
        SingleOutputStreamOperator<SensorReading> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<SensorReading>("late"))
                .sum("tempartare");

        resultStream.print();

        env.execute();
    }
}
