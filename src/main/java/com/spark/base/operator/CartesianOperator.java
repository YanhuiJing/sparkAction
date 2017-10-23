package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.Arrays;


public class CartesianOperator {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext= SparkUtils.getJavaSparkContextInstance();

        JavaRDD<String> beforeRdd = javaSparkContext.parallelize(Arrays.asList("A", "B", "C"));
        JavaRDD<String> afterRdd = javaSparkContext.parallelize(Arrays.asList("D", "E", "F"));

        // 笛卡尔积算子,所得结果为前后算子个数乘积
        JavaPairRDD<String, String> cartesian = beforeRdd.cartesian(afterRdd);

        cartesian.collect().stream().forEach(System.out::println);

        javaSparkContext.close();

    }
}
