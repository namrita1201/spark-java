package com.spark.rdd.action;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeExample {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);

    List<String> stringList = Arrays
        .asList("big", "data", "spark", "JavaAPI", "spark", "spark", "hadoop", "spark", "hive", "pig", "cassandra",
            "hadoop");

    SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> stringRdd = sc.parallelize(stringList);

    final List<String> takeList = stringRdd.take(3);

    for (String element : takeList) {
      System.out.println(element);
    }
  }
}