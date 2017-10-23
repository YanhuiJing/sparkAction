package com.spark.base.sql;

import com.spark.base.model.Student;
import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple3;

import java.util.Arrays;

public class DataFrameCreate {

    public static void main(String[] args) {

       createByRowFactory();

    }

    /**
     * 通过bean创建对应的dataset
     */
    public static void createByReflection(){

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();
        Dataset<Student> studentDF = sparkSession.read().textFile("data/students.txt")
                .map((MapFunction<String, Student>) value -> {

                    String[] split = value.split(",");
                    Student student = Student.builder()
                            .id(Integer.valueOf(split[0]))
                            .name(split[1])
                            .age(Integer.valueOf(split[2]))
                            .build();
                    return student;

                }, Encoders.bean(Student.class));

        studentDF.show();
        sparkSession.close();
    }

    /**
     * 基于这种方式没有没有字段名称
     */
    public static void createByRdd(){

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();
        Dataset<String> dataSource = sparkSession.read().textFile("data/students.txt");

        dataSource.map((MapFunction<String, Tuple3<Integer, String, Integer>>) value -> {
            String[] split = value.split(",");
            return new Tuple3<>(Integer.valueOf(split[0]),split[1],Integer.valueOf(split[2]));
        },Encoders.tuple(Encoders.INT(),Encoders.STRING(),Encoders.INT())).show();

    }

    /**
     * 通过rowFactory指定字段名称和数据类型创建dataset
     */
    public static void createByRowFactory(){

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();
        Dataset<String> dataSource = sparkSession.read().textFile("data/students.txt");

        dataSource.map((MapFunction<String, Row>) value -> {
            String[] split = value.split(",");
            return RowFactory.create(Integer.valueOf(split[0]),split[1],Integer.valueOf(split[2]));
        }, RowEncoder.apply(DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id",DataTypes.IntegerType,true),
                DataTypes.createStructField("name",DataTypes.StringType,true),
                DataTypes.createStructField("score",DataTypes.IntegerType,true)
        )))).show();

        sparkSession.close();

    }
}

