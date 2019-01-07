package com.spark.pairRdd.join;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class InnerJoinExample {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setMaster("local").setAppName("innerJoin");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<String, Integer> ages = sc.parallelizePairs(
        Arrays.asList(new Tuple2<String, Integer>("James", 19), new Tuple2<String, Integer>("John", 30)));

    JavaPairRDD<String, String> address = sc.parallelizePairs(Arrays
        .asList(new Tuple2<String, String>("James", "United States"), new Tuple2<String, String>("Mary", "Australia")));

    JavaPairRDD<String, Tuple2<Integer, String>> joinedPairRdd = ages.join(address);

    joinedPairRdd.saveAsTextFile("out/inner_join_pairRdd");

  }
}
