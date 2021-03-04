package cn.bjfu.testApi;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class MySensorSource implements SourceFunction<SensorReading> {
    //定义一个标志位，用来控制数据的产生
    private boolean running = true;
    public void run(SourceContext<SensorReading> ctx) throws Exception {
           Random random = new Random();
           HashMap<String,Double> map = new HashMap<String, Double>();
           for(int i=1;i<11;i++){
               map.put("sensor"+i,60+random.nextGaussian()*20);
           }
           while (running){
               for(String sensorId:map.keySet()){
                  Double newTemp = map.get(sensorId) + random.nextGaussian();
                  map.put(sensorId,newTemp);
                  ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
               }
               Thread.sleep(1000);
           }
    }

    public void cancel() {
           running = false;
    }
}
