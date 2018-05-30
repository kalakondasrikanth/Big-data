package it.unipd.dei.bdc1718;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


public class G05HM3
{
    public static void main(String[] args) throws IOException
    {

        java.util.ArrayList<Vector> test1;
        java.util.ArrayList<Vector> test2;
        java.util.ArrayList<Vector> test3;
        java.util.ArrayList<Vector> test4;
        
        System.out.println("Welcome to POINT CLUSTER 3000 ");
        System.out.println("Please, select one of the available txt file");
    
        System.out.println("1) test1.txt 10000 points");
        System.out.println("2) test2.txt 50000 points");
        System.out.println("3) test3.txt 100000 points");
        System.out.println("4) test4.txt 500000 points");
        
        ArrayList<Vector> pointSet = new ArrayList<Vector>();
        int numCenter=1;
        int numCenter2=1;
        
        boolean choosing=true;
        while(choosing)
        {
            try
            {
                Scanner userInput = new Scanner(System.in);
                int k = userInput.nextInt();
        
                switch (k)
                {
                    case 1:
                        test1 = InputOutput.readVectorsSeq("D:\\Gian\\Università\\Magistrale\\Secondo Semestre\\Big Data\\HM3\\vecs-50-10000.txt");
                        pointSet = test1;
                        choosing=false;
                        break;
                    case 2:
                        test2 = InputOutput.readVectorsSeq("D:\\Gian\\Università\\Magistrale\\Secondo Semestre\\Big Data\\HM3\\vecs-50-50000.txt");
                        pointSet = test2;
                        choosing=false;
                        break;
                    case 3:
                        test3 = InputOutput.readVectorsSeq("D:\\Gian\\Università\\Magistrale\\Secondo Semestre\\Big Data\\HM3\\vecs-50-100000.txt");
                        pointSet = test3;
                        choosing=false;
                        break;
                    case 4:
                        test4 = InputOutput.readVectorsSeq("D:\\Gian\\Università\\Magistrale\\Secondo Semestre\\Big Data\\HM3\\vecs-50-500000.txt");
                        pointSet = test4;
                        choosing=false;
                        break;
                    default:
                        break;
                }
            } catch (NoSuchElementException e)
            {
                System.out.println("Select a value between 1 and 4");
            }
        }
        
        choosing=true;
        System.out.println("Wonderful!! Now insert how many center do you want me to find(>0)");
        while(choosing)
        {
            try
            {
                Scanner userInput = new Scanner(System.in);
                int k = userInput.nextInt();
                if(k>0)
                {
                    choosing = false;
                    numCenter=k;
                }
                else throw new NoSuchElementException();
            } catch (NoSuchElementException e)
            {
                System.out.println("Try again");
            }
        }
    
        choosing=true;
        System.out.println("Insert the number of center for the second method");
        while(choosing)
        {
            try
            {
                Scanner userInput = new Scanner(System.in);
                int k = userInput.nextInt();
                if(k>0 && k>numCenter)
                {
                    numCenter2=k;
                    choosing = false;
                }
                else throw new NoSuchElementException();
            } catch (NoSuchElementException e)
            {
                System.out.println("Try again");
            }
        }
    
        System.out.println("Loading....");
        ArrayList<Vector> resultKCenter;  //where to store the results
        long tic;
        long toc;
        tic = System.currentTimeMillis();
        resultKCenter=kcenter(pointSet,numCenter);
        toc = System.currentTimeMillis();
        long time1 = toc-tic;
        
        ArrayList<Vector> resultKmeanspp;
        ArrayList<Long> weight = new ArrayList<Long>();
        for(int i = 0; i<pointSet.size();i++)
        {
            weight.add(1L);
        }
        tic = System.currentTimeMillis();
        resultKmeanspp=kmeanspp(pointSet,weight,numCenter2);
        toc = System.currentTimeMillis();
        long time2 = toc-tic;
        
        System.out.println("Time for the first method: "+time1/1000);
        System.out.println("Time for the second method: "+time2/1000);
        
        
        double resultKmeansObj1;
        resultKmeansObj1 = kmeansObj(pointSet,resultKCenter);
        System.out.println("Mean distance1: "+resultKmeansObj1);
        
        double resultKmeansObj2;
        resultKmeansObj2 = kmeansObj(pointSet,resultKmeanspp);
        System.out.println("Mean Distance2: "+resultKmeansObj2);
        
    }
    public static ArrayList<Vector> kcenter(ArrayList<Vector> P,int k)
    {
        System.out.println("Start1");
        ArrayList<Vector> C = new ArrayList<Vector>(); //Will contain the centers (S)
        ArrayList<Vector> freePoint = P;    //Avaible points to choose to be the centers (P-S)
        int arraySize=freePoint.size();

        ArrayList<Double> dist = new ArrayList<Double>(arraySize); //Array that will be used to store the min distance from a freePoint and a center

        Random r = new Random();

        int p = (int) Math.floor(r.nextDouble()*(arraySize-1)); //choosing the first point at random

        Vector goodPoint= freePoint.get(p);

        C.add(goodPoint); //adding the the center to vector of Centers

        Vector lastPoint = goodPoint;
        double tempDist;
        int pos;
        for (int i = 0; i<k-1;i++)
        {
            pos=0;
            for (Vector point : freePoint)
            {
                tempDist = Vectors.sqdist(lastPoint, point); //calculating the distance from the lastPoint choosen and the freePoint
                
                //pos = freePoint.indexOf(point);
                if(i==0)        //if it is the first point initialize the dist vector
                {
                    dist.add(tempDist);
                }
                else
                    {
                    double prevDist = dist.get(pos);
                    if (tempDist < prevDist)  //if the distance of the point to the lastPoint is less than the distance found in the dist arrayList change the value
                    {
                        dist.set(pos, tempDist);
                    }
                }
                pos++;
            }
            double maxDist=Collections.max(dist); //find the max value in the dist ArrayList
            int newPointPos = dist.indexOf(maxDist);
            lastPoint = freePoint.get(newPointPos);
            C.add(lastPoint); //adding the point to the Center
        }
        System.out.println("Finish1");
        return C;
    }

    public static ArrayList<Vector> kmeanspp(ArrayList<Vector> P,ArrayList<Long> weight,int k)
    {
        System.out.println("Start2");
        ArrayList<Vector> C = new ArrayList<Vector>(); //Will contain the centers (S)
        ArrayList<Vector> freePoint = P;    //Available points to choose to be the centers (P-S)
        int arraySize=freePoint.size();
    
        ArrayList<Double> dist = new ArrayList<Double>(arraySize); //Array that will be used to store the min distance from a freePoint and a center
        double den;
        Random r = new Random();
    
        int p = (int) Math.floor(r.nextDouble()*(arraySize-1)); //choosing the first point at random
    
        Vector goodPoint= freePoint.get(p);
    
        C.add(goodPoint); //adding the the center to vector of Centers
    
        Vector lastPoint = goodPoint;
        freePoint.remove(lastPoint);
        double[] prob = new double[arraySize];
        double prevDist;
        double tempDist;
        int pos;
        for(int i =0 ; i<k-1;i++)
        {
            pos=0;
            den=0;
            //calculating dp for each p
            for(Vector point : freePoint)
            {
                tempDist= Vectors.sqdist(lastPoint, point); //calculating the distance from the lastPoint choosen and the freePoint
                //pos= freePoint.indexOf(point);
                if(i==0)        //if it is the first point initialize the dist vector
                {
                    dist.add(tempDist);
                    den = den + tempDist*tempDist*weight.get(pos); //denominator of the prob. function
                }
                else
                {
                     prevDist= dist.get(pos);
                    if (tempDist < prevDist)  //if the distance of the point to the lastPoint is less than the distance found in the dist arrayList change the value
                    {
                        dist.set(pos, tempDist);
                        den = den + tempDist*tempDist*weight.get(pos);
                    }
                    else
                    {
                        den = den + prevDist*prevDist*weight.get(pos); //wronggggg
                    }
                }
                pos++;
            }
        
            for(int j=0;j<dist.size();j++)
            {
                double distance = dist.get(j);
                prob[j]=(distance*distance*weight.get(j))/den;
            }
    
            Random r2 = new Random();
            double select = r2.nextDouble();
    
            boolean cond=true;
            double prob2 =0;
            int cont=0;
            while(cond)
            {

                prob2=prob2+prob[cont];
                
                if(prob2>select)
                {
                    //int position = freePoint.indexOf(cont);
                    C.add(freePoint.get(cont));
                    freePoint.remove(cont);
                    //weight.remove(cont);
                    cond=false;
                }
                else
                {
                    cont = cont+1;
                }
            }
    
        }
        System.out.println("finish2");
    return C;
    }
    
    public static double kmeansObj(ArrayList<Vector> P ,ArrayList<Vector> C)
    {
        System.out.println("Start3");
        double mean=0;
        double min;
        double temp;
        for (int i = 0; i< P.size();i++)
        {
            min = Vectors.sqdist(P.get(i),C.get(0));
            for (int j =1;j<C.size();j++)
            {
                temp = Vectors.sqdist(P.get(i),C.get(j));
                if(temp<min)
                {
                    min=temp;
                }
            }
            mean = mean+min;
        }
        System.out.println("Finish3");
        return mean/P.size();
    }
    
    
}
