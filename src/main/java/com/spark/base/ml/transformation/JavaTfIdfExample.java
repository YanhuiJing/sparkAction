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

import java.util.Arrays;
import java.util.List;

import com.spark.base.util.SparkUtils;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * TF-IDF算法
 * Tokenizer => 对单词进行分词
 * HashingTF => 设置要统计的特征个数,统计词频
 * IDF => 根据词频,统计单词在全文的重要程度
 */
public class JavaTfIdfExample {
  public static void main(String[] args) {
    SparkSession spark = SparkUtils.getSparkSessionInstance();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, "Hi I heard about Spark"),
      RowFactory.create(0.0, "I wish Java could use case classes"),
      RowFactory.create(1.0, "Logistic regression models are neat")
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
    });
    Dataset<Row> sentenceData = spark.createDataFrame(data, schema);

    Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
    Dataset<Row> wordsData = tokenizer.transform(sentenceData);
    wordsData.show(false);

    // 设置要统计的特征个数
    int numFeatures = 20;
    HashingTF hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(numFeatures);

    Dataset<Row> featurizedData = hashingTF.transform(wordsData);
    featurizedData.show(false);
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
    IDFModel idfModel = idf.fit(featurizedData);

    Dataset<Row> rescaledData = idfModel.transform(featurizedData);
    rescaledData.show(false);

    spark.stop();
  }
}
