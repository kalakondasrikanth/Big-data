package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.util.*;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;


public class G05HM2
{
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line!");
        }

        // Spark Setup
        SparkConf conf = new SparkConf(true).setAppName("G05HM2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Loading Data
        JavaRDD<String> documents = sc.textFile(args[0]); // Reading the file given as input
        documents.persist(StorageLevel.MEMORY_AND_DISK()).repartition(16);

        // Performing an action on the RDD to force its loading into memory
        int nDoc = (int) documents.count(); // To avoid lazy evaluation

        // MR Word Count VERSION 0: straightforward approach
        long tic;
        long toc;
        tic = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcount0 = documents.flatMapToPair((document) -> {
                    String[] words = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String word: words)
                        pairs.add(new Tuple2<>(word, 1L));
                    return pairs.iterator();
                })
                .groupByKey().mapValues((iter) -> {
                    long sum = 0;
                    for (long c : iter) {
                        sum += c;
                    }
                    return sum;
                });
        long nWord0 = wordcount0.count(); // To avoid lazy evaluation
        toc = System.currentTimeMillis();
        long time0 = toc - tic;


        // MR Word Count VERSION 1: "Improved Word Count 1"
        tic = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcount1 = documents.flatMapToPair((document) -> { // Map Phase
            String[] words = document.split(" "); // Separating documents into words
            HashMap<String, Tuple2<String, Long>> pairs = new HashMap<>();
            for (String word: words) {
                Tuple2<String, Long> temp = pairs.get(word);
                if (temp == null) // If the word is not present, add it...
                    pairs.put(word, new Tuple2<>(word, 1L));
                else {          // ..otherwise increase its value by 1
                    pairs.replace(word, temp, new Tuple2<>(word, temp._2 + 1L));
                }
            }
            return pairs.values().iterator();
            })
                .groupByKey().mapValues((iter) -> { // Reduce Phase
                    long sum = 0;
                    for (long c : iter) {
                        sum += c;
                    }
                    return sum;
                });
        long nWord1 = wordcount1.count(); // To avoid lazy evaluation
        toc = System.currentTimeMillis();
        long time1 = toc - tic;


        // MR Word Count VERSION 2: "Improved Word Count 2"
        tic = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcount2 = documents.flatMapToPair((document) -> { // Map Phase #1
            String[] words = document.split(" ");
            HashMap<String,Tuple2<String, Long>> pairs = new HashMap<>(); // Using HashMap to speed things up
            for (String word: words) {
                Tuple2<String,Long> temp = pairs.get(word);
                if (temp == null) // If the tuple is not present, add it...
                    pairs.put(word,new Tuple2<>(word, 1L));
                else { // ..otherwise increase its value by 1
                    pairs.replace(word, temp, new Tuple2<>(word, temp._2 + 1L));
                }
            }
            long numWords = pairs.size(); // Number of words N
            long K = (long) Math.ceil(Math.sqrt(numWords)); // sqrt(N)
            ArrayList<Tuple2<Long,Tuple2<String, Long>>> splitPairs = new ArrayList<>(); // (x,(w,ci(w))
            Random r = new Random();
            long x;
            for (Tuple2 token: pairs.values()) {
                x = (long) Math.floor(r.nextDouble()*(K-1)); // Random key in [0, sqrt(N))
                splitPairs.add(new Tuple2(x, token)); // Assigning the key to every tuple
            }
            return splitPairs.iterator();
        })
                .groupByKey()
                .flatMapToPair((tuple) ->{ // Reduce #1
                    HashMap<String,Tuple2<String,Long>> partialCount = new HashMap<>();
                    for (Tuple2<String,Long> tupleToken: tuple._2) {
                        String word = tupleToken._1;
                        long count = tupleToken._2;
                        Tuple2<String,Long> temp = partialCount.get(word); // Similar to the previous flatMapToPair():
                        if (temp == null) // If the tuple is not present, add it..
                            partialCount.put(word,new Tuple2<>(word,count));
                        else { // ..otherwise increase its count by summing the two values
                            partialCount.replace(word, temp, new Tuple2<>(word, temp._2 + count));
                        }
                    }
                    return partialCount.values().iterator();
                })
                .groupByKey().mapValues((iter) -> { // Reduce #2
                    long sum = 0;
                    for (long c : iter) {
                        sum += c;
                    }
                    return sum;
                });
        long nWord2 = wordcount2.count(); // To avoid lazy evaluation
        toc = System.currentTimeMillis();
        long time2 = toc - tic;

        // MR Word Count VERSION 3: using reduceByKey() method
        tic = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcount3 = documents.flatMapToPair((document) -> { // Map Phase
            String[] words = document.split(" ");
            HashMap<String, Tuple2<String, Long>> pairs = new HashMap<>();
            for (String word: words) {
                Tuple2<String, Long> temp = pairs.get(word);
                if (temp == null) // If the word is not present, add it...
                    pairs.put(word, new Tuple2<>(word, 1L));
                else {          // ..otherwise increase its value by 1
                    pairs.replace(word, temp, new Tuple2<>(word, temp._2 + 1L));
                }
            }
            return pairs.values().iterator();
        })
                .reduceByKey((x,y) -> (x + y)); // Reduce Phase
        long nWord3 = wordcount3.count();  // To avoid lazy evaluation
        toc = System.currentTimeMillis();
        long time3 = toc - tic;

        // Printing out the time measurements taken from different versions
        System.out.println("DOCUMENT COUNT WAS PERFORMED. COMPUTED DATA:");
        System.out.println("The number of documents is: " + nDoc);
        System.out.print("Version 0 = found: " + nWord0 + " words in ");
        System.out.printf("%.3f", (double) time0/1000);
        System.out.println(" s");
        System.out.print("Version 1 = found: " + nWord1 + " words in ");
        System.out.printf("%.3f", (double) time1/1000);
        System.out.println(" s");
        System.out.print("Version 2 = found: " + nWord2 + " words in ");
        System.out.printf("%.3f", (double) time2/1000);
        System.out.println(" s");
        System.out.print("Version 3 = found: " + nWord3 + " words in ");
        System.out.printf("%.3f", (double) time3/1000);
        System.out.println(" s");


        // Printing the results
        System.out.print("Please insert the amount of most frequent words you want to see [at most " + nWord3 + "]: ");
        try {
            Scanner userInput = new Scanner(System.in);
            int k = userInput.nextInt(); // Number of results to be shown taken as input
            // Creating a RDD of pairs ordered in descending order by number of occurrences
            JavaPairRDD<Long, String> swappedWordCount = wordcount2.mapToPair((pair) -> pair.swap()).sortByKey(false);
            Iterator iter = swappedWordCount.collect().iterator();
            // Printing the k most frequent words
            System.out.println("The top " + k + " words are:");
            for (int i = 1; i <= k; i++) {
                Tuple2<Long, String> temp = (Tuple2) iter.next();
                String word = temp._2.toUpperCase();
                Long occurrences = temp._1;
                System.out.printf("%-20s", i + ") " + word);
                System.out.println(" with " + occurrences + " occurrences.");
            }
        }
        catch (NoSuchElementException e) {
            System.out.println("Maximum number of words exceeded!");
        }
    }
}