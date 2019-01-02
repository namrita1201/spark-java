package com.spark.rdd.action;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountExample {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.ERROR);

    List<String> stringList = Arrays
        .asList("big", "data", "spark", "JavaAPI", "spark", "spark", "hadoop", "spark", "hive", "pig", "cassandra",
            "hadoop");

    SparkConf conf = new SparkConf().setAppName("count").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> stringRdd = sc.parallelize(stringList);

    //count counts duplicates as well

    System.out.println("Total element count is " + stringRdd.count());

    Map<String, Long> countMap = stringRdd.countByValue();

    System.out.println("Word Count ");

    for (Map.Entry<String, Long> mapEntry : countMap.entrySet()) {
      System.out.println(mapEntry.getKey() + " : " + mapEntry.getValue());
    }
  }
}
