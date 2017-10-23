package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class SampleOperator {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        JavaRDD<String> dataSource = javaSparkContext.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"));

        JavaRDD<String> sample = dataSource.sample(false, 0.5, 101);

        sample.collect().stream().forEach(System.out::println);

        javaSparkContext.close();


    }
}
