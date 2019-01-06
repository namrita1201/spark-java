package com.spark.pairRdd.aggregations.transformations;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class GroupByKeyExample {

  public static void main(String[] args) {

        /* Create a Spark program to read the airport data from resources/airports.txt,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

    SparkConf conf = new SparkConf().setMaster("local").setAppName("grpByKey").set("spark.driver.host", "localhost");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/airports.txt");
    JavaPairRDD<String, String> airportTuple = lines.mapToPair(getAirportCountryAndName());
    JavaPairRDD<String, Iterable<String>> airportGrp = airportTuple.groupByKey();

    final Map<String, Iterable<String>> airportGrpMap = airportGrp.collectAsMap();

    for (Map.Entry<String, Iterable<String>> entry : airportGrpMap.entrySet()) {
      System.out.println(entry.getKey() + " , " + entry.getValue());
    }
  }

  private static PairFunction<String, String, String> getAirportCountryAndName() {
    return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(",")[3], line.split(",")[1]);
  }
}
