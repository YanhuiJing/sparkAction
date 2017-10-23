package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class AggerateByKeyOperator {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext= SparkUtils.getJavaSparkContextInstance();

        JavaRDD<String> dataSource=javaSparkContext.parallelize(Arrays.asList(
                "hadoop spark hive flink",
                "flink spark storm"
        ));

        JavaPairRDD<String, Integer> wordsPairRDD = dataSource.flatMap(data -> {
            String[] words = data.split(" ");
            return Arrays.asList(words).iterator();
        }).mapToPair(word -> {
            return new Tuple2<>(word, 1);
        });

        //aggregateByKey是简化版的shuffle流程
        //第一个参数代表聚合计算的初始值
        //第二个参数代表mapSide聚合过程
        //第三个参数代表reduceSide聚合过程
        JavaPairRDD<String, Integer> aggregateRdd = wordsPairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> collect = aggregateRdd.collect();

        collect.stream().forEach(System.out::println);

        javaSparkContext.close();
    }
}
