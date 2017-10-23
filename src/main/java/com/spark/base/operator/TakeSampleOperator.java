package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author gavin
 * @createDate 2020/3/13
 */
public class TakeSampleOperator {

    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        JavaRDD<String> dataSource = javaSparkContext.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"));

        //基于take算子获取指定随机数量的元素
        List<String> list = dataSource.takeSample(false, 5, 100);

        list.stream().forEach(System.out::println);

        javaSparkContext.close();


    }


}
