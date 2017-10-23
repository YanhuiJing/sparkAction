package com.spark.base.sql;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

/**
 * sql练习
 *
 * @author gavin
 * @createDate 2020/3/3
 */
public class SQLAction {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();


       sparkSession.read().textFile("data/sql/student")
                .map((MapFunction<String, Row>) value -> {
                        String[] splits = value.split(",");
                        return RowFactory.create(splits[0],splits[1],splits[2],splits[3]);
                    },RowEncoder.apply(DataTypes.createStructType(Arrays.asList(
                            DataTypes.createStructField("stu_id",DataTypes.StringType,true),
                            DataTypes.createStructField("stu_name",DataTypes.StringType,true),
                            DataTypes.createStructField("date",DataTypes.StringType,true),
                            DataTypes.createStructField("sex",DataTypes.StringType,true)
                )))).registerTempTable("student");

        sparkSession.read().textFile("data/sql/score")
                .map((MapFunction<String, Row>) value -> {
                    String[] splits = value.split(",");
                    return RowFactory.create(splits[0],splits[1],Integer.valueOf(splits[2]));
                },RowEncoder.apply(DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("stu_id",DataTypes.StringType,true),
                        DataTypes.createStructField("course_id",DataTypes.StringType,true),
                        DataTypes.createStructField("score",DataTypes.IntegerType,true)
                )))).registerTempTable("scores");

        sparkSession.read().textFile("data/sql/course")
                .map((MapFunction<String, Row>) value -> {
                    String[] splits = value.split(",");
                    return RowFactory.create(splits[0],splits[1],splits[2]);
                },RowEncoder.apply(DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("course_id",DataTypes.StringType,true),
                        DataTypes.createStructField("course_name",DataTypes.StringType,true),
                        DataTypes.createStructField("tech_id",DataTypes.StringType,true)
                )))).registerTempTable("course");

        sparkSession.read().textFile("data/sql/teacher")
                .map((MapFunction<String, Row>) value -> {
                    String[] splits = value.split(",");
                    return RowFactory.create(splits[0],splits[1]);
                },RowEncoder.apply(DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("tech_id",DataTypes.StringType,true),
                        DataTypes.createStructField("tech_name",DataTypes.StringType,true)
                )))).registerTempTable("teacher");


        sql03(sparkSession);

        sparkSession.close();

    }

    public static Dataset<Row> sql01(SparkSession sparkSession){

        Dataset<Row> dataSet = sparkSession.sql("select * from student where stu_name like '猴%'");
        dataSet.show();

        return dataSet;
    }

    public static Dataset<Row> sql02(SparkSession sparkSession){

        Dataset<Row> dataset = sparkSession.sql(
                                "select course_id, " +
                                "count(stu_id) as num " +
                                "from scores " +
                                "group by course_id "+
                                "having count(stu_id) <=3 "+
                                "order by num, course_id asc");

        dataset.show();

        return dataset;

    }


    /**
     * 开窗函数 https://www.douban.com/group/topic/155112949/
     * row_number() over()：对相等的值不进行区分，相等的值对应的排名相同，序号从1到n连续。
     *                      90,90,80,70 => 1,2,3,4
     * rank() over()：相等的值排名相同，但若有相等的值，则序号从1到n不连续。如果有两个人都排在第3名，则没有第4名。
     *                      90,90,80,70 => 1,1,3,4
     * dense_rank() over()：对相等的值排名相同，但序号从1到n连续。如果有两个人都排在第一名，则排在第2名（假设仅有1个第二名）的人是第3个人。
     *                      90,90,80,70 => 1,1,2,3
     * ntile( n ) over()：可以看作是把有序的数据集合平均分配到指定的数量n的桶中,将桶号分配给每一行，排序对应的数字为桶号。
     *                   如果不能平均分配，则较小桶号的桶分配额外的行，并且各个桶中能放的数据条数最多相差1
     * @param sparkSession
     * @return
     */
    public static Dataset<Row> sql03(SparkSession sparkSession){

        Dataset<Row> dataset = sparkSession.sql(
                                "select * from " +
                                            "(select stu_id, " +
                                                "course_id, " +
                                                "score, " +
                                                "row_number() over(partition by course_id order by score asc) rank "+
                                            "from scores) temp "+
                                        "where temp.rank<=20");

        dataset.show();

        return dataset;

    }

    /**
     * 通过case when语句完成行列转换
     * @param sparkSession
     * @return
     */
    public static Dataset<Row> sql04(SparkSession sparkSession){

        Dataset<Row> dataset = sparkSession.sql(
                "select "+
                            "course_id, "+
                            "sum(case when score>90 then 1 else 0 end) as 90_count, "+
                            "sum(case when score>80 and score<=90 then 1 else 0 end) as 80_count, "+
                            "sum(case when score>70 and score<=80 then 1 else 0 end) as 70_count, "+
                            "sum(case when score<70 then 1 else 0 end) as 70_count "+
                        "from scores " +
                            "group by course_id");

        dataset.show();
        return dataset;
    }

    /**
     * 淘宝数仓案例
     * store_base_info:(店铺基础信息)
     *      store_id => 店铺id
     *      store_name => 店铺名称
     *      is_tmaall => 1是天猫,0是淘宝
     *      location_city => 所在地区
     *
     * store_credit_info:(店铺信用信息)
     *      store_id =>店铺id
     *      credit_as_seller =>店铺信用
     *      score_goods_desc => 宝贝描述
     *      score_service_manner => 卖家服务态度
     *      score_express_speed => 卖家发货速度
     *      info_update_date => 信息最后更新日期
     *
     * goods_base_info:(商品基本信息)
     *      goods_id => 商品id
     *      goods_name => 商品名称
     *      store_id => 店铺id
     *      class_one => 一级分类
     *      class_two => 二级分类
     *      class_three => 三级分类
     *      info_acquire_date => 信息获取时间
     *      date_add => 上架时间
     *
     * goods_sale_info:(商品交易流水)
     *      goods_id => 商品id
     *      data_date => 数据日期
     *      price => 价格
     *      day_sale_count_total => 当天总销量
     *
     * 1,计算最近7天销量最高的商家
     *      select
     *          c.store_id,c.store_name,sum(a.day_sale_count_total) as sale_total
     *          from goods_sale_info a left join goods_base_info b on a.goods_id = b.goods_id
     *                               left join store_base_info c on b.store_id = c.store_id
     *          where a.data_date >= date_sub(to_date(from_unixtime(unix_timestamp()),7)
     *          group by c.store_id,c.store_name
     *          order by sale_total desc
     *          limit 10
     *
     * 2,计算最近7天上线新品最多的10家店铺
     *      select
     *          b.store_id,b.store_name,count(distinct a.goods_id) as goods_num
     *          from goods_base_info a left join store_base_info b on a.store_id = b.store_id
     *          where a.date_add >= date_sub(to_date(from_unixtime(unix_timestamp()),7)
     *          group by b.store_id,b.store_name
     *          order by goods_num desc
     *          limit 10;
     *
     * 3,计算最近7天相比之前7天销量增长最快的店铺
     *
     *                select
     *                c.store_id,c.store_name,
     *                sum(case when a.data_date >= date_sub(to_date(from_unixtime(unix_timestamp()),7) then day_sale_count_total
     *                         else 0-day_sale_count_total end) as sale_total
     *                from goods_sale_info a left join goods_base_info b on a.goods_id = b.goods_id
     *                                     left join store_base_info c on b.store_id = c.store_id
     *                where a.data_date >= date_sub(to_date(from_unixtime(unix_timestamp())),14)
     *                group by c.store_id,c.store_name
     *                order by sale_total desc
     *                limit 10
     *
     * 4,计算最近一个月新上架的商品中,头七天销量最高的商品
     *
     *          select
     *              b.goods_id,b.goods_name,sum(a.day_sale_count_total) as goods_num
     *          from goods_sale_info a left join goods_base_info b on a.goods_id=b.goods_id
     *          where b.date_add >= date_sub(to_date(from_unixtime(unix_timestamp())),30)
     *                and a.data_date >= b.date_add and a.date <= date_add(b.date_add,7)
     *          group by b.goods_id,b.goods_name
     *          order by goods_num desc
     *          limit 10
     *
     */


}
