package cn.bjfu.testApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class Testform_MulipStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> inputStream = env.addSource(new MySensorSource());

        SplitStream<SensorReading> splitStream = inputStream.split(new OutputSelector<SensorReading>() {

            public Iterable<String> select(SensorReading value) {
                return value.getTemperatue() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        highStream.print("highStream");


        DataStream<SensorReading> lowStream = splitStream.select("low");
        lowStream.print("lowStream");

        //2.合并流 connect 将高温流转换成二元组类型，与低温连接合并之后，输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<String, Double>(value.getId(), value.getTemperatue());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStream = warningStream.connect(lowStream);
        SingleOutputStreamOperator<Object> connectStream = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {

            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3(value.f0, value.f1, "高温报警");
            }

            public Object map2(SensorReading value) throws Exception {
                return new Tuple2(value.getId(), "normal");
            }
        });

        connectStream.print();

        //3.union联合两条流

        env.execute();
    }
}
