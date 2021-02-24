package cn.bjfu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//做一个批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取路径
        String inputPath = "C:\\Users\\Administrator\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt" ;
        DataSource<String> dataSource = env.readTextFile(inputPath);
        //按照空格分词展开，转换成(word,1)进行统计
        AggregateOperator<Tuple2<String, Integer>> resultSet = dataSource.flatMap(new MyflatMapper())
                .groupBy(0).sum(1);//按照第一个位置的word分组

        resultSet.print();
    }
    //自定义类，实现FlatMapFunction接口
    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            //遍历所有word 包成二元组输出
            for(String word:words){
               out.collect( new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
