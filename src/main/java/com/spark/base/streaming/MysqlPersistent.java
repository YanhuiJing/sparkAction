package com.spark.base.streaming;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * sparkStreaming持久化数据到mysql,可以通过rdd数据转dataset,通过sparkSql的方式存入数据库
 * @author gavin
 * @createDate 2020/2/25
 */
public class MysqlPersistent {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf()
                        .setAppName("KafkaReceiverWordCount1")
                        .setMaster("local[2]");

        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(conf, Durations.seconds(5));

        javaStreamingContext.textFileStream("")
                .foreachRDD( rdd ->{
                    SparkContext sparkContext = rdd.rdd().sparkContext();

                    SQLContext sqlContext = new SQLContext(sparkContext);

                    // 数据库连接参数可以通过广播变量
                    Map<String, String> option = new HashMap<>();
                    option.put("url","jdbc:mysql://localhost:3306/sparksql");
                    option.put("dbtable","studentinfo");
                    option.put("user","root");
                    option.put("password","123456");

                    sqlContext.createDataFrame(rdd,Word.class)
                            .write().options(option).save();

                });

    }

}

class MysqlPool implements Serializable{

    private static MysqlPool mysqlPool = null;

    private ComboPooledDataSource comboPooledDataSource = null;

    private MysqlPool(){

        try {

            comboPooledDataSource = new ComboPooledDataSource(true);
            comboPooledDataSource.setJdbcUrl("jdbc:mysql://192.168.57.101:3306/mysql?useUnicode=true&characterEncoding=utf8");
            comboPooledDataSource.setDriverClass("com.mysql.jdbc.Driver");
            comboPooledDataSource.setUser("root");
            comboPooledDataSource.setPassword("root");
            comboPooledDataSource.setMaxPoolSize(200);  //连接池最大连接数量
            comboPooledDataSource.setMinPoolSize(20);   //连接池最小连接数量
            comboPooledDataSource.setAcquireIncrement(5);   //每次递增数量
            comboPooledDataSource.setMaxStatements(180);    //连接池最大空闲时间

        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }

    }


    public Connection getConnection(){

        try {
            return comboPooledDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 连接池单例
     * @return
     */
    public static MysqlPool getMysqlPool(){
        if(mysqlPool == null){
            synchronized (MysqlPool.class){
                if(mysqlPool == null){
                    mysqlPool = new MysqlPool();
                }
            }
        }

        return mysqlPool;
    }
}


class Word{

    private String text;

}