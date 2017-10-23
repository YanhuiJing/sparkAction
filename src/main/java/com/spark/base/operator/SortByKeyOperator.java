package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class SortByKeyOperator {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();


        List<Tuple2<Integer,String>> scoreList= Arrays.asList(
                new Tuple2<>(100,"a"),
                new Tuple2<>(60,"b"),
                new Tuple2<>(50,"c"),
                new Tuple2<>(20,"d"),
                new Tuple2<>(60,"e")
        );

        JavaPairRDD<Integer, String> sortResult = javaSparkContext.parallelizePairs(scoreList).sortByKey(false);

        sortResult.collect().stream().forEach(System.out::println);

        javaSparkContext.close();
    }
}
