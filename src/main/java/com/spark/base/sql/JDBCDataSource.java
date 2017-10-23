package com.spark.base.sql;

import com.spark.base.util.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *         url: jdbc:mysql://java.sdb.roombox.xdf.cn:3306/sac_mgmt?autoReconnect=true&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=CONVERT_TO_NULL&useSSL=false&serverTimezone=CTT
 *         username: sacm_mgmt
 *         password: mgmt_sacm
 *
 *         select
 *     id,
 *     organization_id,
 *     unix_timestamp(start_time) as start_time,
 *     unix_timestamp(end_time) as end_time
 *   from biz_classroom
 *   where
 *     status = 0
 *     and start_time &gt;= #{startDate}
 *     and start_time &lt; DATE_ADD(#{endDate}, INTERVAL 1 DAY)
 */
public class JDBCDataSource {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();

        Map<String, String> option = new HashMap<>();
        option.put("url","jdbc:mysql://java.sdb.roombox.xdf.cn:3306/sac_mgmt?autoReconnect=true&useUnicode=true&characterEncoding=utf8");
        option.put("dbtable","biz_classroom");
        option.put("user","sacm_mgmt");
        option.put("password","mgmt_sacm");

        Dataset<Row> dataset = sparkSession.read().format("jdbc").options(option).load();

        dataset.show(10);


//        mysqlSource.write().format("jdbc").options(option).save();

    }
}
