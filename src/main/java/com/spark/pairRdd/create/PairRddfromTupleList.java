package com.spark.pairRdd.create;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRddfromTupleList {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("createPaidRdd").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<String, Integer>> tuple2List = Arrays
        .asList(new Tuple2<>("Namrita", 37), new Tuple2<>("Anshul", 37), new Tuple2<>("Anvi", 6));

    JavaPairRDD<String, Integer> pairRDDFromTuple = sc.parallelizePairs(tuple2List);

    //to combine the results from all partitions coalesce is used
    pairRDDFromTuple.coalesce(1).saveAsTextFile("out/paiRdd_from_tuple.txt");

  }
}
