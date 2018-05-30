package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Tuple2;

public class G05HM3
{
    public static void main(String[] args) throws FileNotFoundException, IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line!");
        }

        // Spark Setup
        SparkConf conf = new SparkConf(true).setAppName("G05HM3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read input file passed via command line
        ArrayList<Vector> P = InputOutput.readVectorsSeq(args[0]);

        System.out.println("POINT ONE.");
        System.out.print("Insert the number k of centers to return: ");
        Scanner userInput = new Scanner(System.in);
        int k = userInput.nextInt();
        long tic = System.currentTimeMillis();
        ArrayList<Vector> Ckc = kcenter(P, k);
        long kcenterTime = System.currentTimeMillis() - tic;
        System.out.println("kcenter(P, k) was computed in: " + kcenterTime + "ms\n");

        System.out.println("POINT TWO.");
        ArrayList<Long> WP = new ArrayList<>();
        for (int i = 0; i < P.size(); i++)
            WP.add((long) 1);
        ArrayList<Vector> Ckmpp = kmeansPP(P, WP, k);
        double d = kmeansObj(P, Ckmpp);
        System.out.print("kmeansObj returns: ");
        System.out.printf("%.3f", d);
        System.out.println("\n");

        System.out.println("POINT THREE.");
        System.out.print("Insert the number of centers k1 > k required to compute the new kmeansObj: ");
        int k1 = userInput.nextInt();
        while (k1 <= k){
            System.out.print("k1 has to be greater than k. Please insert it again: ");
            k = userInput.nextInt();
        }
        userInput.close();
        ArrayList<Vector> X = kcenter(P, k1);
        ArrayList<Long> WX = new ArrayList<>();
        for (int i = 0; i < X.size(); i++)
            WX.add((long) 1);
        ArrayList<Vector> C = kmeansPP(X, WX, k);
        double result = kmeansObj(P, C);
        System.out.print("kmeansObj returns: ");
        System.out.printf("%.3f", result);
        System.out.print("\n");
        if (result < d)
            System.out.println("Point 3 has performed BETTER than point 2!");
        else
            System.out.println("Point 3 has performed WORSE than point 2.");
    }


    // Auxiliary methods kcenter(P,k), kmeansPP(P,WP,k), kmeansObj(P,C)

    public static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k) {
        ArrayList<Vector> C = new ArrayList<Vector>(); // Set of centers to be returned
        ArrayList<Vector> P1 = P; // Will be P\C
        int rand = (int) Math.floor(Math.random()*(P.size()-1)); // Generating a random integer in [ 0,|P|)
        Vector c1 = P.get(rand);
        C.add(c1); // Adding a random point in the set added as first center
        P1.remove(c1); // .. and removing it from P/C

        /* The folloging ArrayList, distances, will keep track
         * of the minimum distances between any point in P1 = P/C and the points in C*/
        ArrayList<Tuple2<Vector, Double>> distances = new ArrayList<>();
        for (int index = 0; index < P1.size(); index++){ // O(|P/C|) = O(|P|-1) = O(P)
            Vector temp = P1.get(index);
            distances.add(new Tuple2<>(temp, Vectors.sqdist(c1, temp))); // Initialization
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
                double dist = Vectors.sqdist(P1.get(j), ci);
                if (dist < distances.get(j)._2)
                    distances.set(j, new Tuple2<>(distances.get(j)._1, dist));
            }
        }

        return C; // Total complexity O(|P|) + O(k*2|P|) = O(|P|*k)
    }

    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k){
        ArrayList<Vector> C = new ArrayList<Vector>();
        ArrayList<Vector> P1 = P; // Will be P\C
        int rand = (int) Math.floor(Math.random()*(P.size()-1)); // Generating a random integer in [0,|P|)
        Vector c1 = P.get(rand); // First arbitrary center
        C.add(c1);
        P1.remove(c1); // Removing the center from P/C..
        WP.remove(rand); // ..and removing the relative weight

        ArrayList<Tuple2<Vector, Double>> dp = new ArrayList<>(); // dp(i) = min{d[C<-c, P(i)]}
        for (int index = 0; index < P1.size(); index++){ // O(|P/C|) = O(|P|-1) = O(P)
            Vector temp = P1.get(index);
            dp.add(new Tuple2<>(temp, Vectors.sqdist(c1, temp)));
        }

        for (int i = 2; i <= k; i++){
            double dq = 0; // Weighted sum of P(i)^2 for each i in [0, |P/C|)
            for (int j = 0; j < P1.size(); j++){ // O(|P/C|) < O(|P|)
                dq += Math.pow(dp.get(j)._2, 2)*WP.get(j);
            }
            ArrayList<Double> probabilities = new ArrayList<>();
            for (int j = 0; j < P1.size(); j++){ // O(|P/C|) < O(|P|)
                probabilities.add((WP.get(j)*dp.get(j)._2)/dq); // Computing single probabilities
            }

            // Using CDF to get a random selection with the above defined distribution
            int posi = 0; // Position of the random center
            double cumulative = 0;
            rand = (int) Math.floor(Math.random()); // Random seed in (0,1)
            while(true) {
               cumulative += probabilities.get(posi);
               if (cumulative >= rand)
                   break; // Stop when CDF < random seed
               posi++;
            }
            Vector ci = P1.get(posi); // Randomly chosen center
            C.add(ci);
            P1.remove(posi);
            WP.remove(posi);
            dp.remove(posi);
            for (int j = 0; j < P1.size(); j++){ // Updating minimum distances: O(|P/C|) < O(|P|)
                double dist = Vectors.sqdist(P1.get(j), ci);
                if (dist < dp.get(j)._2)
                    dp.set(j, new Tuple2<>(dp.get(j)._1, dist));
            }
        }

        return C; // Total complexity O(|P|) + O(k*3|P|) = O(|P|*k)
    }

    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C){
        int k = C.size(); // k = |C|
        int cardP = P.size(); // |P|
        // Minimum distances between each point in P and the set C
        ArrayList<Double> squaredDistances = new ArrayList<Double>();
        Vector c1 = C.get(0);
        for (int i = 0; i < cardP; i++){ // Initialization of squareDistances: O(|P|)
            squaredDistances.add(Math.pow(Vectors.sqdist(P.get(i), c1), 2));
        }
        for (int i = 1; i < k; i++){ // O(k)
            Vector ci = C.get(i);
            for (int j = 0; j < cardP; j++){ // O(|P|)
                double dist = Math.pow(Vectors.sqdist(P.get(j), ci), 2);
                if (dist < squaredDistances.get(j)) // If it's a new minimum, update the value
                    squaredDistances.set(j, dist);
            }
        }

        double result = 0;
        for (int i = 0; i < cardP; i++)
            result += squaredDistances.get(i)/cardP; // Sum of squared distances divided by |P|
        return result; // Total complexity O(|P|) + O(k*|P|) + O(|P|) = O(|P|*k)
    }
}
