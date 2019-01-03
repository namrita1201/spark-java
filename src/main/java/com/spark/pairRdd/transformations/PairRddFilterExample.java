package com.spark.pairRdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairRddFilterExample {

  public static void main(String[] args) {

    /*     Create a Spark program to read the airport data from resources/airports.txt;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
    */

    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("filterPair").setMaster("local").set("spark.driver.host", "localhost");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/airports.txt");

    JavaPairRDD<String, String> pairRdd = lines.mapToPair(getAirportNameAndCity());

    final JavaPairRDD<String, String> airportNotInUSA = pairRdd
        .filter((keyValue) -> !(keyValue._2.equals("\"United States\"")));

    airportNotInUSA.saveAsTextFile("out/pairRdd_filter_airport_notInUSA");
  }

  private static PairFunction<String, String, String> getAirportNameAndCity() {
    return (PairFunction<String, String, String>) s -> new Tuple2<>(s.split(",")[1], s.split(",")[3]);
  }
}
