package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;


public class CountByKeyOperator {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        JavaPairRDD<String, Integer> pairRdd = javaSparkContext.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 80),
                new Tuple2<>("b", 80),
                new Tuple2<>("c", 80),
                new Tuple2<>("a", 80),
                new Tuple2<>("b", 80),
                new Tuple2<>("d", 80)
        ));

        Map<String, Long> keyMap = pairRdd.countByKey();
        System.out.println(keyMap);

        javaSparkContext.close();
    }
}
