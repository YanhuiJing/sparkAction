package com.spark.base.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author gavin
 * @createDate 2020/4/11
 */
public class MapWithStateAction {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("mapWithStateAction");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        jsc.checkpoint("checkPointpath");

        // mapWithState函数只有对应的key有新的数据时,才会触发计算逻辑,否则没有对应的输出
        // mapWithState输入参数包括key,value,以及对应的状态值,状态值类型可以是多种各类型
        // mapWithState状态可以通过StateSpec指定对应key的失效时间
        Function3<String, Optional<Integer>, State<Integer>, String> function3=
        (key, value, state) -> {

            Integer sum = value.orElse(0) + (state.exists() ? state.get() : 0);
            state.update(sum);

            return key + ":" + sum;
        };

        jsc.socketTextStream("localhost", 9999)
                .flatMap(data -> Arrays.asList(data.split("\\W")).iterator())
                .mapToPair(data -> new Tuple2<>(data, 1))
                .mapWithState(StateSpec.function(function3).timeout(Durations.minutes(24*60L)))
                .print();

        jsc.start();
        jsc.awaitTermination();


    }


}
