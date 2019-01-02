package com.spark.rdd.action;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbers {

  public static void main(String[] args) {

        /* Create a Spark program to read the first 100 prime numbers from resources/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("sumOfNumbers").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> inputRdd = sc.textFile("src/main/resources/prime_nums.txt");

    JavaRDD<String> numberString = inputRdd.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

    JavaRDD<String> validNumbers = numberString.filter(number -> !number.isEmpty());

    JavaRDD<Integer> numberRdd = validNumbers.map(Integer::valueOf);

    System.out.println("Sum of prime number is " + numberRdd.reduce((x, y) -> x + y));

  }
}
