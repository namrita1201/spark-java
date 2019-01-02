package com.spark.pairRdd.create;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

public class PairRddFromRegularRdd {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("createPaidRdd").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<String> inputList = Arrays.asList("Lily 23", "James 90", "Mary 89", "Joe 29");

    JavaRDD<String> regularRdd = sc.parallelize(inputList);

    JavaPairRDD<String, Integer> javaPairRDD = regularRdd.mapToPair(getNameAndAgePair());

    javaPairRDD.coalesce(1).saveAsTextFile("out/pairRdd_from_regular_Rdd");
  }

  private static PairFunction<String, String, Integer> getNameAndAgePair() {
    return (PairFunction<String, String, Integer>) s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
  }
}
