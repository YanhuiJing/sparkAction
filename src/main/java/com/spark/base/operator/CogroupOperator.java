package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class CogroupOperator {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        List<Tuple2<Integer,String>> nameList= Arrays.asList(
                new Tuple2<Integer,String>(1,"tanjie"),
                new Tuple2<Integer,String>(2,"zhangfan"),
                new Tuple2<Integer,String>(2,"tanzhenghua"),
                new Tuple2<Integer,String>(3,"lincangfu")
        );

        List<Tuple2<Integer,Integer>> scoreList=Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,60),
                new Tuple2<Integer, Integer>(3,90),
                new Tuple2<Integer, Integer>(1,70),
                new Tuple2<Integer, Integer>(2,50),
                new Tuple2<Integer, Integer>(3,40)

        );

        JavaPairRDD<Integer, String> nameRdd = javaSparkContext.parallelizePairs(nameList);
        JavaPairRDD<Integer, Integer> scoreRdd = javaSparkContext.parallelizePairs(scoreList);

        //cogroup算子基于key将前后rdd的值都进行聚合
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = nameRdd.cogroup(scoreRdd);
        cogroup.collect().stream().forEach(System.out::println);

        javaSparkContext.close();

    }
}
