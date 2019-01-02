package com.spark.rdd.action;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectExample {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);

    List<String> stringList = Arrays.asList("big", "data", "spark", "JavaAPI", "rdd", "transformations", "action");

    SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> stringRdd = sc.parallelize(stringList);

    List<String> listCreatedByCollect = stringRdd.collect();

    for (String element : listCreatedByCollect) {
      System.out.println(element);
    }
  }
}
