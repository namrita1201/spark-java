package com.spark.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkSQLInAction {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    SparkSession session = SparkSession.builder().appName("sparkSQLExamples").master("local[2]").getOrCreate();

    DataFrameReader dataFrameReader = session.read();

    Dataset<Row> rowDataset = dataFrameReader.option("header", true)
        .csv("src/main/resources/2016-stack-overflow-survey-responses.csv");

    //Example 1 - print the schema

    rowDataset.printSchema();

    //Example 2 - print first 20 rows

    rowDataset.show(20);

    //Example 3 - select so_region and self_identification columns

    rowDataset.select(col("so_region"), col("self_identification")).show();

    //Example 4 - select rows where response id from Africa

    rowDataset.filter(col("country").equalTo("Afghanistan")).show();

    //Example 5 - print the count of occupation

    RelationalGroupedDataset groupedDataset = rowDataset.groupBy(col("occupation"));
    groupedDataset.count().show();

  }
}
