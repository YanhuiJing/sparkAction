package com.spark.base.streaming;

import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

/**
 * https://blog.csdn.net/daerzei/article/details/80085121
 * https://blog.csdn.net/feloxx/article/details/70789000
 * https://www.jianshu.com/p/d2a61be73513
 */
public class KafkaOffsetByRedis {
    public static void main(String[] args) throws InterruptedException {

        //sparkStreaming高可用模式,需要将程序的全部逻辑包含进去
        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate("", KafkaOffsetByRedis::getJavaStreamingContext);

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();


    }

    public static JavaStreamingContext getJavaStreamingContext(){
        SparkConf conf=new SparkConf().setAppName("KafkaReceiverWordCount1")
                .setMaster("local[5]");
        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", "192.168.57.101:9092");
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", "gavin");
        //指定从latest，还是earliest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);

        List<String> topics = Arrays.asList("test");

        Map<TopicPartition,Long> offsetMap = RedisOffsetUitils.getInstance().getTopicOffset("gavin");

        // 从topic的指定偏移量开始消费
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsetMap)
        );


        directStream.transform( rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            BroadCastWrapperOffset.getInstance().updateAndget(rdd.rdd().sparkContext(),offsetRanges);
            return rdd.map(data  ->new Tuple2(data.value(),"1"));
        }).foreachRDD(rdd -> {
            Broadcast broadcast = BroadCastWrapperOffset.getInstance().getBroadcast();

            persisRedis(rdd,"localhost",60 * 60 * 1000);

            RedisOffsetUitils.getInstance().commitRangeOffset("gavin",(OffsetRange[])broadcast.getValue());

//            使用kafka自己存储offset,使用的前提是将enable.auto.commit设置为false
//            ((CanCommitOffsets)directStream.inputDStream()).commitAsync((OffsetRange[])broadcast.getValue());
        });


        return javaStreamingContext;

    }

    /**
     * 通过redisLab持久化rdd数据
     */
    public static void persisRedis(JavaRDD javaRDD,String hostname,int ttl){

        RDD<Tuple2<String,String>> rdd = javaRDD.rdd();
        SparkContext sparkContext = rdd.sparkContext();

        RedisContext redisContext = new RedisContext(sparkContext);
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkContext.getConf());
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkContext.getConf());

        redisContext.toRedisHASH(rdd,hostname,ttl,redisConfig,readWriteConfig);

    }

    /**
     * 根据kafka指定分区偏移量获取数据
     * @param javaStreamingContext
     * @param kafkaParams
     */
    public static void getKafkaRdd(JavaStreamingContext javaStreamingContext,Map kafkaParams){

        OffsetRange[] offsetRanges = {
                // topic, partition, inclusive starting offset, exclusive ending offset
                //参数依次是Topic名称，Kafka哪个分区，开始位置(偏移)，结束位置
                OffsetRange.create("test", 0, 0, 100),
                OffsetRange.create("test", 1, 0, 100)
        };

        JavaRDD javaRDD = KafkaUtils.createRDD(
                javaStreamingContext.sparkContext(),
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );

    }
}

class RedisOffsetUitils{

    private static RedisOffsetUitils redisOffsetUitils = new RedisOffsetUitils();

    private final String SEPARATION_SYMBOL = "|";

    private RedisOffsetUitils(){};

    public static RedisOffsetUitils getInstance(){
        return redisOffsetUitils;
    }

    public Jedis getRedis(){

        return new Jedis("localhost",6379);

    }

    public Map<TopicPartition,Long> getTopicOffset(String groupId){
        Map<String, String> map = getRedis().hgetAll(groupId);
        Map<TopicPartition,Long> offsetMap = new HashMap<>();

        if(Objects.nonNull(map)){
            for(Map.Entry<String,String> entry:map.entrySet()){
                String[] split = entry.getKey().split(SEPARATION_SYMBOL);
                offsetMap.put(new TopicPartition(split[0],Integer.valueOf(split[1])),Long.valueOf(entry.getValue()));
            }
        }

        return offsetMap;

    }

    public void commitRangeOffset(String groupId,OffsetRange[] offsetRanges){

        for(OffsetRange offsetRange:offsetRanges){

            String key = offsetRange.topic() + SEPARATION_SYMBOL + offsetRange.partition();
            String value = String.valueOf(offsetRange.untilOffset());

            getRedis().hset(groupId,key,value);

        }


    }

}

class BroadCastWrapperOffset{
    private Broadcast<OffsetRange[]> broadcast;

    private long lastUpdateTime = 0L;

    private static BroadCastWrapperOffset broadCastWrapper = new BroadCastWrapperOffset();

    private BroadCastWrapperOffset(){};

    /**
     * 获取
     */
    public Broadcast<OffsetRange[]> updateAndget(final SparkContext sparkContext, final OffsetRange[] offsetRanges){

            if(Objects.nonNull(broadcast)){
                broadcast.unpersist();
            }

            broadcast = JavaSparkContext.fromSparkContext(sparkContext).broadcast(offsetRanges);

            return broadcast;

    }

    /**
     * 获取广播变量
     */
    public Broadcast getBroadcast(){

        return broadcast;

    }

    public static BroadCastWrapperOffset getInstance(){
        return broadCastWrapper;
    }

}

