package com.spark.rdd.action;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReduceExample {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.ERROR);

    List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5);

    SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Integer> inputRdd = sc.parallelize(integerList);

    System.out.println("The product of numbers in the list is " + inputRdd.reduce((x, y) -> x * y));

  }
}
