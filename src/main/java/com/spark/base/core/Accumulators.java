package com.spark.base.core;

import com.spark.base.util.SparkUtils;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.Collections;

/**
 * spark2.0以上版本内置三种累加器,LongAccumulator(数值累加)、DoubleAccumulator(小数累加)、CollectionAccumulator（集合累加）
 * 累加器相关blog:https://www.cnblogs.com/cc11001100/p/9901606.html
 */
public class Accumulators {
    public static void main(String[] args) throws InterruptedException {

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();


        LongAccumulator total = sparkSession.sparkContext().longAccumulator("total");
        DoubleAccumulator doubleAccumulator = sparkSession.sparkContext().doubleAccumulator();
        CollectionAccumulator<Object> collectorAccumulator = sparkSession.sparkContext().collectionAccumulator();

        sparkSession.createDataset(Collections.singletonList(1024), Encoders.INT())
                .foreach((ForeachFunction<Integer>) total::add);

        Thread.sleep(120000);

        sparkSession.close();

    }
}
