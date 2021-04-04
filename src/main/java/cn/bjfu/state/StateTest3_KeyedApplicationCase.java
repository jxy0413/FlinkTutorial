package cn.bjfu.state;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by jxy on 2021/4/4 0004 18:46
 */
public class StateTest3_KeyedApplicationCase {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamContextEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStream = sEnv.addSource(new MySensorSource());
        //定义flatMap操作，监测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(5.0));
        //resultStream.print();
        sEnv.execute();
    }

    //实现自定义函数
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>> {
        //私有属性，温度跳变阈值
        private Double treashold;
        public TempChangeWarning(double treashold ){
            this.treashold = treashold;
        }

        //定义状态，保存上一次温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            //获取状态
            Double lastTemp = lastTempState.value();
            //如果不为null，那么就判断两次温度差值
            if(lastTemp != null){
                Double diff  = Math.abs(value.getTemperatue()-lastTemp);
                if(diff>=treashold){
                    collector.collect(new Tuple3<String, Double, Double>(value.getId(),lastTemp,value.getTemperatue()));
                }
            }
            //更新状态
            lastTempState.update(value.getTemperatue());
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
