package cn.bjfu.state;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by jxy on 2021/4/4 0004 18:19
 */
public class StateTest2_keyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamContextEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> myStream = sEnv.addSource(new MySensorSource());
        //定义一个有状态的map,统计当前sonsor个数
        SingleOutputStreamOperator<Integer> stream = myStream.keyBy("id")
                .map(new MyKeyCountMappper());
        stream.print();
        sEnv.execute();
    }

    public static class MyKeyCountMappper extends RichMapFunction<SensorReading,Integer>{
        private ValueState<Integer> keyCountState;

        private ListState<String> myListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list",String.class));
        }

        public Integer map(SensorReading value) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
