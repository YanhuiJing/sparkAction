package com.spark.base.sql;

import com.spark.base.util.SparkUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


public class DataFrameFunctions {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();

        Dataset<Row> df = sparkSession.read().json("data/json/students.json");

        df.show();

        df.printSchema();

        // 通过select获取指定字段
        df.select("name").show();

        // import static org.apache.spark.sql.functions.*; 静态导入sqlFunction,直接调用对应的方法
        df.select(col("name"), col("score").plus(1).as("score_plus")).show();

        df.filter(col("score").gt(95)).show();

        df.groupBy(col("score")).count().show();

        // 临时表只能在创建的对应session中使用
        df.createOrReplaceTempView("stu");

        sparkSession.sql("select * from stu").show();

        // 全局表的生命周期贯穿sparkSession对话全程
        df.createOrReplaceGlobalTempView("g_stu");

        sparkSession.sql("select * from global_temp.g_stu").show();

        sparkSession.newSession().sql("select * from global_temp.g_stu").show();

        //基于sqlFunction实现caseWhen
        df.select(when(col("score").$greater(90), 0)
                .when(col("score").$greater(60), 1)
                .otherwise(2).as("level")).show();

        sparkSession.close();
    }
}
