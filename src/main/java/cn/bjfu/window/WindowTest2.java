package cn.bjfu.window;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowTest2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        SingleOutputStreamOperator<Double> id = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MvAvgTemp());

        id.print();

        env.execute();
    }

    public static class MvAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double,Integer>,Double>{
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<Double, Integer>(0.0,0);
        }

        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<Double, Integer>(accumulator.f0+value.getTemperatue(),accumulator.f1+1);
        }

        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<Double, Integer>(a.f0+b.f0,a.f1+b.f1);
        }
    }
}
