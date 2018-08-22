package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;;
import scala.Tuple2;

public class G05HM4 {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line!");
        }

        // Spark Setup
        SparkConf conf = new SparkConf(true).setAppName("G05HM4");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read input file passed via command line
        System.out.println("POINT ONE.");
        System.out.print("Insert the number k of points to extract from each subset: ");
        Scanner userInput = new Scanner(System.in);
        int k = userInput.nextInt();
        System.out.print("Insert the number numBlocks for pointsrdd partitioning: ");
        int numBlocks = userInput.nextInt();
        System.out.print("\n");
        userInput.close();

        System.out.println("POINT TWO.");
        System.out.println("Performing the required computations and printing the results..");
        JavaRDD<Vector> pointsdd = sc.textFile(args[0]).map(x->strToVector(x)).repartition(numBlocks).cache();
        ArrayList<Vector> rmrResult = runMapReduce(pointsdd,k,numBlocks);

        double distance = measure(rmrResult);
        System.out.print("2.1) The average distance among the solution points is: ");
        System.out.printf("%.3f", distance);
        System.out.print("\n");

    }

    /**
     * Function required by point one.
     */
    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsrdd, int k, int numBlocks) {
        // Subpoint a): rdd already repartitioned before calling this method

        // Subpoint b): running Farthest First Traversal [i.e. kcenter(P,k)] on every partition
        long tic = System.currentTimeMillis();
        JavaRDD<Vector> pointsrdd1 = pointsrdd.mapPartitions((Iterator<Vector> iter) ->{
            ArrayList<Vector> list = new ArrayList<>();
            while (iter.hasNext()) {
                list.add(iter.next());
            }
            ArrayList<Vector> centers = kcenter(list,k);
            return centers.iterator();
        });

        // Subpoint c): collecting the numBlocks*k points from the rdd
        ArrayList<Vector> coreset = new ArrayList<Vector>(pointsrdd1.collect());
        long toc = System.currentTimeMillis() - tic;
        System.out.println("2.2) The time taken by coreset construction [size equal to numBlocks*k = " +
                coreset.size() + "] is: " + toc + " ms.");

        // Subpoint d): running the sequential approximation algorithm
        tic = System.currentTimeMillis();
        ArrayList<Vector> rmrResult = runSequential(coreset, k);
        toc = System.currentTimeMillis() - tic;
        System.out.println("2.3) The time taken by the sequential algorithm is: " + toc + " ms.");

        return rmrResult;
    }

    /**
     * Function required by point two.
     */
    static double measure(ArrayList<Vector> pointlist){
        int k = pointlist.size();
        int total = k*(k-1)/2;
        double result = 0;
        for (int i = 0; i < k - 1; i++){
            for (int j = i + 1; j < k; j++){ // Directly summing up the distances
                result += Math.sqrt(Vectors.sqdist(pointlist.get(i),pointlist.get(j)));
            }
        }
        result = result/total; // Getting the final result by dividing for k*(k-1)/2

        return result;
    }

    /**
     * Farthest First Traversal algorithm from Homework #3.
     */
    public static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k) {
        ArrayList<Vector> C = new ArrayList<Vector>(); // Set of centers to be returned
        ArrayList<Vector> P1 = P; // Will be P\C
        int rand = (int) Math.floor(Math.random()*(P.size()-1)); // Generating a random integer in [0,|P|)
        Vector c1 = P.get(rand);
        C.add(c1); // Adding a random point in the set added as first center
        P1.remove(c1); // .. and removing it from P/C

        ArrayList<Tuple2<Vector, Double>> distances = new ArrayList<>();
        for (int index = 0; index < P1.size(); index++){ // O(|P/C|) = O(|P|-1) = O(P)
            Vector temp = P1.get(index);
            distances.add(new Tuple2<>(temp, Math.sqrt(Vectors.sqdist(c1, temp))));
        }

        for (int i = 2; i <= k; i++){ // O(k)
            double maxDist = 0; // Maximum of all the minimum distances between each point in P1 and the set C
            int maxInd = 0;
            for (int j = 0; j < P1.size(); j++){ // O(|P/C|) < O(|P|)
                if (distances.get(j)._2 > maxDist)
                    maxInd = j;
            }
            Vector ci = distances.get(maxInd)._1; // The new center is the one at maximum distance
            C.add(ci);
            P1.remove(maxInd);
            distances.remove(maxInd);

            for (int j = 0; j < P1.size(); j++){ // Updating minimum distances: O(|P/C|) < O(|P|)
                double dist = Math.sqrt(Vectors.sqdist(distances.get(j)._1, ci));
                if (dist < distances.get(j)._2)
                    distances.set(j, new Tuple2<>(distances.get(j)._1, dist));
            }
        }

        return C; // Total complexity O(|P|) + O(k*2|P|) = O(|P|*k)
    }



    /**
     * Sequential approximation algorithm based on matching.
     */
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {
        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter = 0; iter < k / 2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i + 1; j < n; j++) {
                        if (candidates[j]) {
                            double d = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }

    /**
     * Conversion from String to Vector,
     * taken from InputOutput class provided for the last Homework
     */
    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

}
