package cn.bjfu.SinkTest;

import cn.bjfu.testApi.MySensorSource;
import cn.bjfu.testApi.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SinkTest3_es {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());
        List<HttpHost> httpHosts = new ArrayList();
        httpHosts.add(new HttpHost("Worker1",9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyEs()).build());

        env.execute();
    }
}
class MyEs implements ElasticsearchSinkFunction<SensorReading>{
    public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
         //定义写入的数据source
        HashMap<String, String> dataSource = new HashMap<String, String>();
        dataSource.put("id",element.getId());
        dataSource.put("temp",element.getTemperatue().toString());
        dataSource.put("ts",element.getTemperatue().toString());

        IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("readingdata").source(dataSource);
        //用index发送请求
        indexer.add(indexRequest);
    }
}