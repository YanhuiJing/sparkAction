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
import org.apache.spark.sql.SparkSession;

// $example on$
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 数据特征归一化
 */
public class JavaStandardScalerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkUtils.getSparkSessionInstance();

    Dataset<Row> dataFrame =
      spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    dataFrame.show(false);

    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false);

    // Compute summary statistics by fitting the StandardScaler
    StandardScalerModel scalerModel = scaler.fit(dataFrame);

    // Normalize each feature to have unit standard deviation.
    Dataset<Row> scaledData = scalerModel.transform(dataFrame);
    scaledData.select("scaledFeatures").show(false);
    // $example off$
    spark.stop();
  }
}
