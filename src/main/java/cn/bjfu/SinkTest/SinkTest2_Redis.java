package cn.bjfu.SinkTest;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkTest2_Redis {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("Worker3")
                .setPort(6379)
                .setPassword("bjfu1022")
                .build();
        dataStream.addSink(new RedisSink<SensorReading>(conf,new MyRedisMapper()));
        env.execute();
    }
  }
class MyRedisMapper implements RedisMapper<SensorReading> {
    //定义保存数据到Redis的命令 存成Hash表 hset sensor_temp id temperature
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
    }

    public String getKeyFromData(SensorReading data) {
        return data.getId();
    }

    public String getValueFromData(SensorReading data) {
        return data.getTemperatue().toString();
    }
}
