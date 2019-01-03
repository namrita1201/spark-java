package com.spark.pairRdd.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairRddMapValuesExample {

  public static void main(String[] args) {
        /* Create a Spark program to read the airport data from resources/airports.txt, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */

    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("mapValues")
        .set("spark.driver.host", "localhost");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/airports.txt");

    JavaPairRDD<String, String> pairRDD = lines.mapToPair(getAirportNameAndCountry());

    JavaPairRDD<String, String> countryNameUpperCase = pairRDD.mapValues(String::toUpperCase);

    countryNameUpperCase.saveAsTextFile("out/pairRdd_value_map_upperCase");
  }

  private static PairFunction<String, String, String> getAirportNameAndCountry() {
    return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(",")[1], line.split(",")[3]);
  }
}
