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
 * RDD Fliter操作算子
 */
public class FliterOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("FliterOperator")
                .setMaster("local[2]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        //filter算子是过滤，里面的逻辑如果返回的是true就保留袭来，false就过滤掉
        JavaRDD<Integer> filterRDD = numbersRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        filterRDD.coalesce(2);//filter算子之后一般都接一个coalesce算子

        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
