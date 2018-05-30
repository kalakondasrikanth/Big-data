package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Comparator;
import java.io.Serializable;

public class G05HM1
{

  public static void main(String[] args) throws FileNotFoundException
  {
    if (args.length == 0)
    {
      throw new IllegalArgumentException("Expecting the file name on the command line");
    }

    // Read a list of numbers from the program options
    ArrayList<Double> lNumbers = new ArrayList<>();
    Scanner s =  new Scanner(new File(args[0]));
    while (s.hasNext())
    {
      lNumbers.add(Double.parseDouble(s.next()));
    }
    s.close();

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setAppName("Preliminaries");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a parallel collection
    JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);

    // Computing the arithmetic mean
    int N = (int) dNumbers.count(); // Total number of elements in the JavaRDD
    double average = dNumbers.map((x) -> x/N).reduce((x, y) -> x + y);
    System.out.println("The average is: " + average);

    // Constructing the dDiffavgs JavaRDD object
    JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> {
      Double value = Math.abs(x-average); // Absolute value of the difference between each element and the average
      return value;
    });

    // Minimum value in dDiffavgs using the reduce method
    double minimum1 = dDiffavgs.reduce((x, y) -> {
        if ( x < y)
            return x;
        else
            return y;
    });
      /* Minimum value in dDiffavgs using the min method.
      The doubleComparator() class implementing comparator is after the main method */
    double minimum2 = dDiffavgs.min(new doubleComparator());

    // Printing the results of the "min" computations
    System.out.println("The minimum computed using the REDUCE method is: " + minimum1);
    System.out.println("The minimum computed using the MIN method is: " + minimum2);

    // Additional statistic using JavaRDD methods: counting the multiples of three in dNumbers
    double multiplesOf3 = dNumbers.filter((x) -> x % 3 == 0).count();
    if (multiplesOf3 == 1)
        System.out.println( (int) multiplesOf3 + " element in the dataset is divisible by 3.");
    else
        System.out.println( (int) multiplesOf3 + " elements in the dataset are divisible by 3.");
  }

    // Class used in the second "min" method
    public static class doubleComparator implements Serializable, Comparator<Double>
    {

        public int compare(Double num1, Double num2)
        {
            if (num1 < num2) return -1;
            else return 1;
        }

    }


}
