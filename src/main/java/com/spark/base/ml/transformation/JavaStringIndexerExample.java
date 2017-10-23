/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spark.base.ml.transformation;

import com.spark.base.util.SparkUtils;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * 获取指定列的全局索引号,索引号默认是按照指定列内容出现的频率进行排序,频率最高的为0
 * 也可以指定字母顺序进行排序
 */
public class JavaStringIndexerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkUtils.getSparkSessionInstance();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0, "a"),
      RowFactory.create(1, "b"),
      RowFactory.create(2, "c"),
      RowFactory.create(3, "a"),
      RowFactory.create(4, "a"),
      RowFactory.create(5, "c")
    );
//    | id|category|categoryIndex|
//    +---+--------+-------------+
//    |  0|       a|          0.0|
//    |  1|       b|          2.0|
//    |  2|       c|          1.0|
//    |  3|       a|          0.0|
//    |  4|       a|          0.0|
//    |  5|       c|          1.0|
    StructType schema = new StructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("category", StringType, false)
    });
    Dataset<Row> df = spark.createDataFrame(data, schema);

//    默认是frequencyDesc,可以根据需要进行指定
//    *   - 'frequencyDesc': descending order by label frequency (most frequent label assigned 0)
//    *   - 'frequencyAsc': ascending order by label frequency (least frequent label assigned 0)
//    *   - 'alphabetDesc': descending alphabetical order
//    *   - 'alphabetAsc': ascending alphabetical order
    StringIndexer indexer = new StringIndexer()
//      .setStringOrderType("alphabetDesc")
      .setInputCol("category")
      .setOutputCol("categoryIndex");

    Dataset<Row> indexed = indexer.fit(df).transform(df);
    indexed.show();

    spark.stop();
  }
}
