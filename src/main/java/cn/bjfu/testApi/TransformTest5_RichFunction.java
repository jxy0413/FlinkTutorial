package cn.bjfu.testApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest5_RichFunction {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();
        env.execute();
    }

    //实现自定义类

}
class MyMapper extends RichMapFunction<SensorReading,Tuple2<String,Integer>> {
    public Tuple2<String, Integer> map(SensorReading value) throws Exception {
        return new Tuple2<String, Integer>(value.getId(),getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化工作
        System.out.println("open");
    }

    @Override
    public void close() throws Exception {

        System.out.println("close");
    }
}
