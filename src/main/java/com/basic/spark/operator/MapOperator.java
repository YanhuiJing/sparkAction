package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/23.
 * RDD Map操作算子
 */
public class MapOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapOperator")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        //map对每个元素进行操作
        JavaRDD<Integer> mapRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 10;
            }
        });

        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }
}
