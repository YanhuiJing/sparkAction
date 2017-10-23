package com.spark.base.structstreaming;

import com.spark.base.util.SparkUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = SparkUtils.getSparkSessionInstance();

        Dataset<Row> dataset = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = dataset.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        /**
         * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
         * <ul>
         * <li> `append`: only the new rows in the streaming DataFrame/Dataset will be written to
         * the sink.</li> 不支持aggregation聚合模式
         * <li> `complete`: all the rows in the streaming DataFrame/Dataset will be written to the sink
         * every time there are some updates.</li>
         * <li> `update`: only the rows that were updated in the streaming DataFrame/Dataset will
         * be written to the sink every time there are some updates. If the query doesn't
         * contain aggregations, it will be equivalent to `append` mode.</li>
         * </ul>
         */
        StreamingQuery query = words.groupBy("value")
                .count()
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
