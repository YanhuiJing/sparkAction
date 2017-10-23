package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class MapPartitionWithIndexOperator {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        JavaRDD<String> dataSource = javaSparkContext.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f"), 3);


        JavaRDD<String> javaRDD = dataSource.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                List<String> list = new ArrayList<>();

                while (v2.hasNext()) {
                    String value = v1 + v2.next();
                    list.add(value);
                }

                return list.iterator();
            }
        },true);

        javaRDD.collect().stream().forEach(System.out::println);

        javaSparkContext.close();

    }
}
