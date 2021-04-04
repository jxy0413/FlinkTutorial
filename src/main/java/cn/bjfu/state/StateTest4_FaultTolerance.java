package cn.bjfu.state;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by jxy on 2021/4/4 0004 19:59
 */
public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamContextEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStream = sEnv.addSource(new MySensorSource());
        //1、状态后端配置
        sEnv.setStateBackend(new MemoryStateBackend());
        sEnv.setStateBackend(new FsStateBackend(""));
        sEnv.setStateBackend(new RocksDBStateBackend(""));
        dataStream.print();
        sEnv.execute();
    }
}
