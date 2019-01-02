package com.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SetOperationUnionExample {

  public static void main(String[] args) {
    /*     "resources/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "resources/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           The original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Ensure that the head lines are removed in the resulting RDD.
         */
    SparkConf conf = new SparkConf().setAppName("setOperationUnion").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> julyFirstLogs = sc.textFile("src/main/resources/nasa_19950701.tsv");
    JavaRDD<String> augustFirstLogs = sc.textFile("src/main/resources/nasa_19950801.tsv");

    JavaRDD<String> aggregatedLogs = julyFirstLogs.union(augustFirstLogs);

    JavaRDD<String> sampledLogs = aggregatedLogs.sample(false, 0.1);

    JavaRDD<String> logsWithoutHeader = sampledLogs.filter(SetOperationUnionExample::isNotHeader);

    logsWithoutHeader.saveAsTextFile("out/sample_nasa_logs.tsv");
  }

  private static boolean isNotHeader(String line) {
    return !(line.startsWith("host") && line.contains("bytes"));
  }
}