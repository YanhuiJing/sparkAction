package com.spark.base.operator;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;


public class TakeSample {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContextInstance();


        JavaRDD<Class> classJavaRDD = javaSparkContext.parallelize(Arrays.asList(
                                                    new Class(1,"2020-03-08","一年级"),
                                                    new Class(3,"2020-03-08","二年级"),
                                                    new Class(8,"2020-03-08","三年级")
                                                            ));

        JavaRDD<StudentEvent> studentEventJavaRDD = javaSparkContext.parallelize(Arrays.asList(
                                                    new StudentEvent(1  ,11 , "Mac" ,"click"),
                                                    new StudentEvent(1  ,12 , "iPhone" ,"click"),
                                                    new StudentEvent(3  ,15 , "Mac" ,"click"),
                                                    new StudentEvent(3  ,16 , "iPade" ,"click")));

        JavaPairRDD<Integer, String> classGradeRDD = classJavaRDD.mapToPair(new PairFunction<Class, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Class aClass) throws Exception {
                return new Tuple2<>(aClass.getId(), aClass.getGrade());
            }
        });


        JavaPairRDD<Integer, Tuple2<Integer, String>> classStudentRDD = studentEventJavaRDD.mapToPair(new PairFunction<StudentEvent, Integer, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, String>> call(StudentEvent studentEvent) throws Exception {
                return new Tuple2<>(studentEvent.getClassId(), new Tuple2<>(studentEvent.getStudentId(), studentEvent.getDevice()));
            }
        }).distinct();

        JavaPairRDD<String, Tuple2<Integer, String>> stringTuple2JavaPairRDD = classGradeRDD.leftOuterJoin(classStudentRDD)
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, Optional<Tuple2<Integer, String>>>>, String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<String, Tuple2<Integer, String>> call(Tuple2<Integer, Tuple2<String, Optional<Tuple2<Integer, String>>>> tuple2) throws Exception {
                        Tuple2<Integer, String> studentInfo = tuple2._2._2.get();

                        return new Tuple2(tuple2._2, new Tuple2<>(studentInfo._1, studentInfo._2));
                    }
                });

        JavaPairRDD<String, Integer> gradeStudentCountRdd = stringTuple2JavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, String>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, String>> tuple2) throws Exception {

                return new Tuple2<>(tuple2._1, 1);
            }
        }).reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Tuple2<String, Integer>>  gradeDeviceCountRdd= stringTuple2JavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, String>>, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<Integer, String>> tuple2) throws Exception {
                return new Tuple2<>(new Tuple2<>(tuple2._1, tuple2._2._2), 1);
            }
        }).reduceByKey((a, b) -> a + b)
        .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1._1, new Tuple2<>(tuple2._1._2, 1));
            }
        });

        gradeStudentCountRdd.join(gradeDeviceCountRdd)
                .map(new Function<Tuple2<String, Tuple2<Integer, Tuple2<String, Integer>>>, Tuple3<String,String,Double>>() {
                    @Override
                    public Tuple3<String, String, Double> call(Tuple2<String, Tuple2<Integer, Tuple2<String, Integer>>> v1) throws Exception {

                        String grade = v1._1;
                        String device = v1._2._2._1;
                        Integer studentCount = v1._2._1;
                        Integer deviceCount = v1._2._2._2;

                        double rate = deviceCount*0.1/studentCount;


                        return new Tuple3<>(grade,device,rate);
                    }
                }).collect();

    }
}


class Class implements Serializable {

    private int id;
    private String date;
    private String grade;

    public Class(int id, String date, String grade) {
        this.id = id;
        this.date = date;
        this.grade = grade;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }
}

class StudentEvent implements Serializable{
    private int classId;
    private int studentId;
    private String device;
    private String event;

    public StudentEvent(int classId, int studentId, String device, String event) {
        this.classId = classId;
        this.studentId = studentId;
        this.device = device;
        this.event = event;
    }

    public int getClassId() {
        return classId;
    }

    public void setClassId(int classId) {
        this.classId = classId;
    }

    public int getStudentId() {
        return studentId;
    }

    public void setStudentId(int studentId) {
        this.studentId = studentId;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }
}
