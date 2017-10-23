package com.spark.base.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author gavin
 * @createDate 2020/4/11
 */
public class DynamicsBroadcast {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("dynamicsBroadcast");

        JavaStreamingContext javaStreamingContext =
                new JavaStreamingContext(sparkConf, Durations.seconds(5));

        javaStreamingContext.socketTextStream("localhost",9999)
                .flatMap(line -> Arrays.asList(line.split("\\W")).iterator())
                .transform(rdd -> {
                    // 动态定时更新广播变量
                    Broadcast broadcast = BroadCastWrapper.getInstance().updateAndget(rdd.rdd().sparkContext(),10*60*1000L,() -> getBroadcastValue());
                    return rdd.filter(data -> ((HashMap<String,String>)broadcast.value()).containsKey(data));
                }).foreachRDD(rdd -> {
                    Broadcast broadcast = BroadCastWrapper.getInstance().getBroadcast();
                    rdd.map(data -> {
                        return ((HashMap<String,String>)broadcast.value()).get(data);
                    }).saveAsTextFile("resPath");
                }
        );

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }


    public static Map<String,String> getBroadcastValue(){

        Jedis jedis = new Jedis("localhost",6379);

        return jedis.hgetAll("broadcastValue");

    }

}

class BroadCastWrapper<T>{

    private Broadcast<T> broadcast;

    private long lastUpdateTime = 0L;

    private static BroadCastWrapper broadCastWrapper = new BroadCastWrapper();

    private BroadCastWrapper(){};

    /**
     * 比较当前时间与上次更新时间,如果超过指定时间间隔,则更新广播变量,否则直接返回
     */
    public Broadcast<T> updateAndget(final SparkContext sparkContext,final long interval,final Function function){

        if((System.currentTimeMillis() - lastUpdateTime) > interval){
            if(Objects.nonNull(broadcast)){
                broadcast.unpersist();
            }

            T value = (T)function.run();
            lastUpdateTime = System.currentTimeMillis();
            return JavaSparkContext.fromSparkContext(sparkContext).broadcast(value);
        }

        return broadcast;

    }

    /**
     * 获取广播变量
     */
    public Broadcast getBroadcast(){

        return broadcast;

    }

    public static BroadCastWrapper getInstance(){
        return broadCastWrapper;
    }

    /**
     * 定义获取广播变量的逻辑,可以从mysql数据库或者redis动态获取
     */
    public interface Function<T>{
        T run();
    }

}
