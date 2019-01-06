package com.spark.pairRdd.aggregations.transformations;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortByKeyExample {
  public static void main(String[] args) {

    /* Create a Spark program to read the an article from resources/word_count.txt,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR);
    SparkConf conf = new SparkConf().setMaster("local").setAppName("sortByKey");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/word_count.txt");
    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    JavaPairRDD<String, Integer> wordTuple = words.mapToPair(word -> new Tuple2<>(word, 1));
    JavaPairRDD<String, Integer> wordCountPair = wordTuple.reduceByKey((x, y) -> x + y);

    JavaPairRDD<Integer, String> countWordPair = wordCountPair
        .mapToPair(wrdCountPair -> new Tuple2<>(wrdCountPair._2(), wrdCountPair._1()));

    JavaPairRDD<Integer, String> sortedCountWordPair = countWordPair.sortByKey(false);

    JavaPairRDD<String, Integer> sortedWordCountPair = sortedCountWordPair
        .mapToPair(wrdCountPair -> new Tuple2<>(wrdCountPair._2(), wrdCountPair._1()));

    for (Tuple2<String, Integer> wordToCount : sortedWordCountPair.collect()) {
      System.out.println(wordToCount._1() + " : " + wordToCount._2());
    }
  }
}
