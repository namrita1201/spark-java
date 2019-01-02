package com.spark.rdd.transformations;

import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.ERROR);
    SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/word_count.txt");
    final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    final Map<String, Long> wordCounts = words.countByValue();

    for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
      System.out.println(entry.getKey() + " : " + entry.getValue());
    }

  }
}
