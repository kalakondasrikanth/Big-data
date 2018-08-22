package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

public class SecondHomework {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> rdd = sc.textFile("text-sample.txt");

        /**
         * The function collect, will get all the elements in the RDD into memory for us to work with them.
         * For this reason it has to be used with care, specially when working with large RDDs.
         * The function collect return a List
         */

        JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> finalCounts = counts.filter((x) -> x._1().contains("@"))
                .collect();

        for (Tuple2<String, Integer> count : finalCounts)
            System.out.println(count._1() + " " + count._2());

        /**
         * This function allow to compute the number of occurrences for a particular word, the first instruction flatMap allows to create the key of the map by splitting
         * each line of the JavaRDD. Map to pair do not do anything because it only define that a map will be done after the reduce function reduceByKey.
         *
         */

        counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        /**
         * This function allows you to filter the JavaPairRDD for all the elements that the number
         * of occurrences are bigger than 20.
         */

        counts = counts.filter((x) -> x._2() > 0);

        long time = System.currentTimeMillis();
        long countEntries = counts.count();
        System.out.println(countEntries + ": " + String.valueOf(System.currentTimeMillis() - time));

        /**
         * The RDDs can be save to a text file by using the saveAsTextFile function which export the RDD information to a text representations
         */
        counts.saveAsTextFile("result1.txt");
        sc.close();

    }

}

