package com.spark.base.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


/**
 * spark工具类
 *
 * @author gavin
 * @createDate 2020/2/23
 */
public class SparkUtils {

    public static SparkSession getSparkSessionInstance(){

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("example")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("warn");

        return sparkSession;
    }

    public static JavaSparkContext getJavaSparkContextInstance(){

        SparkSession sparkSession = getSparkSessionInstance();
        return JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

    }


}
