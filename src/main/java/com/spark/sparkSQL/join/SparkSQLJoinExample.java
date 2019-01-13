package com.spark.sparkSQL.join;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLJoinExample {

  /*
  Write a Spark program to find the distribution of postal code by region
   */

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkSession session = SparkSession.builder().master("local").appName("sparkSQLJoins").getOrCreate();

    Dataset<Row> makerSpaceDataset = session.read().option("header", true)
        .csv("src/main/resources/uk-makerspaces-identifiable-data.csv");

    Dataset<Row> postalCode = session.read().option("header", true).csv("src/main/resources/uk-postcode.csv");

    Dataset<Row> postalCodeAltered = postalCode.withColumn("PostCode", concat_ws("", col("Postcode"), lit(" ")));

    final Dataset<Row> joinedDataset = makerSpaceDataset
        .join(postalCodeAltered, makerSpaceDataset.col("PostCode").startsWith(postalCodeAltered.col("PostCode")),
            "left_outer");

    joinedDataset.groupBy("Region").count().show();
  }
}
