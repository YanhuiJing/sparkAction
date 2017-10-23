package com.spark.base.core;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;

/**
 * 自定义累加器accumulatorV2
 *
 * @author gavin
 * @createDate 2020/2/23
 */
public class AccumulatorsDefine {

    public static void main(String[] args) throws InterruptedException {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();

        JavaRDD<Integer> dataSource = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        AddAccumulator addAccumulator = new AddAccumulator();

        javaSparkContext.sc().register(addAccumulator);

        dataSource.map(data -> {
            addAccumulator.add(data);
            return data;
        }).collect();

        System.out.println(addAccumulator.value());

        Thread.sleep(120000);

        javaSparkContext.close();

    }




}

class AddAccumulator extends AccumulatorV2<Integer,Integer>{

    private Integer total = 0;

    @Override
    public boolean isZero() {
        return total == 0;
    }

    @Override
    public AccumulatorV2<Integer, Integer> copy() {

        AddAccumulator addAccumulator = new AddAccumulator();
        addAccumulator.total = total;
        return addAccumulator;

    }

    @Override
    public void reset() {
        total = 0;
    }

    @Override
    public void add(Integer v) {
        this.total = this.total + v;

    }

    @Override
    public void merge(AccumulatorV2<Integer, Integer> other) {
        this.total = this.total + other.value();

    }

    @Override
    public Integer value() {
        return this.total;
    }
}