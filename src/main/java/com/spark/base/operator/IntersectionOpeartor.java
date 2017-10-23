package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class IntersectionOpeartor {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        JavaRDD<String> beforeRdd = javaSparkContext.parallelize(Arrays.asList("a", "b", "c", "d"));

        JavaRDD<String> afterRdd = javaSparkContext.parallelize(Arrays.asList("a", "b", "c", "e"));

        //获取集合的交集,变相可通过join实现
        JavaRDD<String> intersection = beforeRdd.intersection(afterRdd);

        intersection.collect().stream().forEach(System.out::println);

        javaSparkContext.close();
    }
}
