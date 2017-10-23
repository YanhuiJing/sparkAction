package com.spark.base.sql;

import com.spark.base.util.SparkUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * DataSet中NaFunction使用案例
 * https://www.cnblogs.com/cc11001100/p/9954862.html
 * @author gavin
 * @createDate 2020/2/23
 */
public class DataFrameNaFunction {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();

        List<Row> rowList = new ArrayList<>();

        for(int i=0;i<100;i++){

            Row row = RowFactory.create(randomValue(i), randomValue(i));
            rowList.add(row);

        }

        Dataset<Row> dataSource = sparkSession.createDataset(rowList, RowEncoder.apply(DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("col_1", DataTypes.IntegerType, true),
                DataTypes.createStructField("col_2", DataTypes.IntegerType, true)
                ))));

        dataSource.show(false);

        DataFrameNaFunctions dataFrameNaFunctions = dataSource.na();

        /*----------------------------- drop -------------------------------*/

        // 只要某行中有一列是null或NaN即丢掉此行数据，内部调用了drop("any")
        dataFrameNaFunctions.drop().show();
        // 指定丢弃行的方式，any表示行中任意一列是null或NaN即丢弃此行，all表示此行中所有列都是null或NaN才丢弃此行
        dataFrameNaFunctions.drop("any").show();
        // 当某行中的所有列为null或NaN时丢弃掉此行
        dataFrameNaFunctions.drop("all").show();
        // 当某行的指定列为null或any时丢弃掉此行
        dataFrameNaFunctions.drop(new String[]{"col_1", "col_2"}).show();
        // 当某行的指定列任意一个为null或NaN时丢弃掉此行
        dataFrameNaFunctions.drop("any", new String[]{"col_1", "col_2"}).show();
        // 当某行的指定列全部为null或NaN时丢弃掉此行
        dataFrameNaFunctions.drop("all", new String[]{"col_1", "col_2"}).show();
        // 当某行中指定列为null或NaN的数量大于指定值时丢弃掉此行
        dataFrameNaFunctions.drop(1).show();
        dataFrameNaFunctions.drop(1, new String[]{"col_1", "col_2"}).show();

        /*----------------------------- fill -------------------------------*/

        // 使用指定的值填充所有为null或NaN的列s，相当于为所有null或NaN设置默认值
        dataFrameNaFunctions.fill(1L).show();
        dataFrameNaFunctions.fill(0.1).show();
        dataFrameNaFunctions.fill("").show();
        dataFrameNaFunctions.fill(true).show();

        // 当给定的列出现null或NaN值时使用对应值填充，相当于为指定的列设置默认值
        dataFrameNaFunctions.fill(1L, new String[]{"col_1, col_2"}).show();
        dataFrameNaFunctions.fill(0.1, new String[]{"col_1, col_2"}).show();
        dataFrameNaFunctions.fill("", new String[]{"col_1, col_2"}).show();
        dataFrameNaFunctions.fill(true, new String[]{"col_1, col_2"}).show();

        // 传入Map可以为每一列设置不同的值，map的key为列名，值为当key列为null或NaN时要填充的值
        // 要填充的值必须是下列类型之一： `Integer`, `Long`, `Float`, `Double`, `String`, `Boolean`.
        dataFrameNaFunctions.fill(ImmutableMap.of("col_1", "unknown", "col_2", 1.0)).show();

        /*----------------------------- replace -------------------------------*/

        // 当指定列的值为key时，将其替换为value
        dataFrameNaFunctions.replace("col_1", ImmutableMap.of("UNKNOWN", "unnamed")).show();
        dataFrameNaFunctions.replace(new String[]{"col_1", "col_2"}, ImmutableMap.of("UNKNOWN", "unnamed")).show();

        sparkSession.close();


    }

    private static Integer randomValue(int num){
        if(Math.random() < 0.5){
            return num;
        }else{
            return null;
        }
    }

}
