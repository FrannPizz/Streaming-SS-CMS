import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;

public class G35HW2 {

    //initialize static variables
    static int P = 8191;
    static int[] aVals;
    static int[] bVals;
    static int n;
    static int d;
    static int w;
    static double phi;

    //count exactly every element in the actual batch with bruteforce
    public static void updateExactCount(List<String> batch, HashMap<Integer, Integer> exactCount){
        //for every element in the batch
        for(String s : batch){
            //parse string to int
            int intS = Integer.parseInt(s);
            //if is in the map count+1, else put it into the map with count=1
            if (exactCount.containsKey(intS)){
                exactCount.put(intS, exactCount.get(intS)+1);
            }else{
                exactCount.put(intS, 1);
            }
        }
    }

    //apply sticky sample to the actual batch of the stream
    public static void updateFSS(List<String> batch, HashMap<Integer, Integer> FSS, double r){
        //for every element in the batch
        for(String s : batch){
            //parse string to int
            int intS = Integer.parseInt(s);
            //if is in the map count+1, else put it into the map with count=1 with prob r/n
            if (FSS.containsKey(intS)){
                FSS.put(intS, FSS.get(intS)+1);
            }else if(Math.random() <= r/n){
                FSS.put(intS, 1);
            }
        }
    }

    //apply Count Min Sketch to the actual batch of the stream
    public static void updateCMS(List<String> batch,int[][] cmsMatrix, HashSet<Integer> FCM){
        //for every element in the batch
        for(String s : batch){
            //parse string to int
            int intS = Integer.parseInt(s);
            //set min to the max=n
            int min = n;
            //update the counter in the cmsMatrix
            for(int j = 0; j < d; j++){
                //optimization, one hashFunction call
                int col = hashFunction(intS, j, w);
                //increase counter
                cmsMatrix[j][col] += 1;
                //update min
                if (cmsMatrix[j][col] < min) {
                    min = cmsMatrix[j][col];
                }
            }
            //add the element only if the min count is >= phi*n
            if(min >= phi*n) {
                FCM.add(intS);
            }
        }
    }

    //hash function for every value of a & b
    public static int hashFunction(int streamItem, int row, int w) {
        long h = Math.floorMod((long) aVals[row] * streamItem + bVals[row], P);
        return (int) Math.floorMod(h, w);
    }

    public static void main(String[] args) throws Exception {

        //parsing args and check
        n = Integer.parseInt(args[0]);
        phi = Double.parseDouble(args[1]);
        double epsilon = Double.parseDouble(args[2]);
        double delta = Double.parseDouble(args[3]);
        d = Integer.parseInt(args[4]);
        w = Integer.parseInt(args[5]);
        int portExp = Integer.parseInt(args[6]);

        if (phi <= 0 || phi >= 1) {
            throw new IllegalArgumentException("phi must be in (0,1)");
        }
        if (epsilon <= 0 || epsilon >= phi) {
            throw new IllegalArgumentException("epsilon must be in (0,phi)");
        }
        if (delta <= 0 || delta >= 1) {
            throw new IllegalArgumentException("delta must be in (0,1)");
        }

        //calculate r parameter for Sticky Sampling
        double r = Math.log(1 / (delta * phi)) / epsilon;

        //set all aVals and bVals
        aVals = new int[d];
        bVals = new int[d];
        for (int i = 0; i < d; i++){
            aVals[i] = (int)(Math.random()*(P - 1) )+1;
            bVals[i] = (int)(Math.random()*(P));
        }

        //mute Spark log
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //initialize Spark
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]")
                .setAppName("G35HW2");

        //set streaming batch of 100ms
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        //set semaphore for stop streaming
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        //inizialize long vector for using iot in Lamda function
        long[] streamLength = new long[1];
        streamLength[0] = 0L;

        //hash table for the distinct elements for Sticky Sample
        HashMap<Integer, Integer> FSS = new HashMap<>();

        //hash table for the distinct elements for exactCount
        HashMap<Integer, Integer> exactCount = new HashMap<>();

        //initialize cmsMatrix
        int[][] cmsMatrix = new int[d][w];
        HashSet<Integer> FCM = new HashSet<>();

        //start socket streaming
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {

                    //check if stram lenght is in our threshold
                    if (streamLength[0] < n) {
                        //count batch size and add it to the counter
                        long batchSize = batch.count();
                        //check batch is not null
                        if (batchSize > 0) {
                            //set remaining elements to process for optimization
                            long remaining = n-streamLength[0];
                            long toProcess = Math.min(batchSize, remaining);

                            //parse batch into list
                            List<String> streamItems = batch.take((int) toProcess);

                            //call the 3 counting methods
                            updateExactCount(streamItems, exactCount);
                            updateFSS(streamItems, FSS, r);
                            updateCMS(streamItems, cmsMatrix, FCM);
                            //add streamLength the processed length
                            streamLength[0] += toProcess;
                            //if the streamLenght is >+ n sto streaming
                            if (streamLength[0] >= n) {
                                //stop receiving and processing further batches
                                stoppingSemaphore.release();
                            }
                        }
                    }
                });
        sc.start();
        stoppingSemaphore.acquire();
        sc.stop(false, false);

        //print for space between real output and Spark logs
        System.out.println();
        System.out.println();
        System.out.println();
        //print arguments
        System.out.println("INPUT PARAMETERS");
        System.out.println("n = " + args[0]);
        System.out.println("phi = " + args[1]);
        System.out.println("epsilon = " + args[2]);
        System.out.println("delta = " + args[3]);
        System.out.println("d = " + args[4]);
        System.out.println("w = " + args[5]);
        System.out.println("port = " + args[6]);
        System.out.println();

        //print results for every algorithm
        //true frequent items, print items with exactCount >= phi*n, sorted by item
        System.out.println("TRUE FREQUENT ITEMS");
        exactCount.entrySet().stream()
                .filter(e -> e.getValue() >= phi * n)
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(e -> System.out.println("Item = " + e.getKey() + " True Freq = " + e.getValue()));

        System.out.println();
        //FSS frequent items, print items in FSS sorted by item and the true frequency
        System.out.println("STICKY SAMPLING");
        System.out.println("Size of dictionary = " + FSS.size());

        FSS.entrySet().stream()
                .filter(e -> e.getValue() >= (phi-epsilon) * n)
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(e -> System.out.println(("Item = " + e.getKey() + " True Freq = " + exactCount.getOrDefault(e.getKey(), 0))));

        System.out.println();
        //FCM frequent items, print items in FCM sorted by item and the true frequency
        System.out.println("COUNT-MIN SKETCH");
        System.out.println("Size of F_CM = " + FCM.size());
        FCM.stream()
                .sorted()
                .forEach(item -> System.out.println(("Item = " + item + " True Freq = " + exactCount.getOrDefault(item, 0))));
    }
}
