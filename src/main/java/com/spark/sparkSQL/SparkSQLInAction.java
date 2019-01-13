package com.spark.sparkSQL;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLInAction {

  public static final String AGE_MIDPOINT = "age_midpoint";
  public static final String SALARY_MIDPOINT = "salary_midpoint";
  public static final String SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket";

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);

    //SparkSession internally has a SparkContext for actual computation
    SparkSession session = SparkSession.builder().appName("sparkSQLExamples").master("local[2]").getOrCreate();

    //DataFrameReader is the Spark interface used to load a dataset from external storage systems
    DataFrameReader dataFrameReader = session.read();

    final Dataset<Row> rowDataset = dataFrameReader.option("header", true)
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

    //Example 6 - cast the salary and age as Integer
    Dataset<Row> rowDataSetWithCast = rowDataset.withColumn(AGE_MIDPOINT, col(AGE_MIDPOINT).cast("integer"))
        .withColumn(SALARY_MIDPOINT, col(SALARY_MIDPOINT).cast("integer"))
        .select(col(AGE_MIDPOINT), col(SALARY_MIDPOINT), col("country"));
    rowDataSetWithCast.show(20);

    //Example 7 - print the records with average mid age less than 20
    rowDataSetWithCast.filter(col(AGE_MIDPOINT).$less(20)).select(col(AGE_MIDPOINT)).show(20);

    //Example 8 - print the records by salary middle point in descending order
    rowDataSetWithCast.orderBy(col(SALARY_MIDPOINT).desc()).select(SALARY_MIDPOINT).show();

    //Example 9 - print the average salary middle point and maximum age for each country
    RelationalGroupedDataset datasetGroupByCountry = rowDataSetWithCast.groupBy("country");
    datasetGroupByCountry.agg(avg(SALARY_MIDPOINT), max(AGE_MIDPOINT)).show();

    //Example 10 - count the number of developers in salary bubket 0-20000, 20000-40000,40000-60000 and so on
    final Dataset<Row> castRowDatasetWithSalaryBucket = rowDataSetWithCast
        .withColumn(SALARY_MIDPOINT_BUCKET, col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))
        .select(col(SALARY_MIDPOINT), col(SALARY_MIDPOINT_BUCKET));

    castRowDatasetWithSalaryBucket.show();
    //group the salaries acc to bucket and count
    castRowDatasetWithSalaryBucket.groupBy(col(SALARY_MIDPOINT_BUCKET)).count().orderBy(col(SALARY_MIDPOINT_BUCKET))
        .show();

    session.stop();
  }
}
