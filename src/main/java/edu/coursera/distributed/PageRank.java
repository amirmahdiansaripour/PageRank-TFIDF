package edu.coursera.distributed;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A wrapper class for the implementation of a single iteration of the iterative
 * PageRank algorithm.
 */
public final class PageRank {
    /**
     * Default constructor.
     */
    private PageRank() {
    }

    /**
     * TODO Given an RDD of websites and their ranks, compute new ranks for all
     * websites and return a new RDD containing the updated ranks.
     *
     * Recall from lectures that given a website B with many other websites
     * linking to it, the updated rank for B is the sum over all source websites
     * of the rank of the source website divided by the number of outbound links
     * from the source website. This new rank is damped by multiplying it by
     * 0.85 and adding that to 0.15. Put more simply:
     *
     *   new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking to B
     *
     * For this assignment, you are responsible for implementing this PageRank
     * algorithm using the Spark Java APIs.
     *
     * The reference solution of sparkPageRank uses the following Spark RDD
     * APIs. However, you are free to develop whatever solution makes the most
     * sense to you which also demonstrates speedup on multiple threads.
     *
     *   1) JavaPairRDD.join
     *   2) JavaRDD.flatMapToPair
     *   3) JavaPairRDD.reduceByKey
     *   4) JavaRDD.mapValues
     *
     * @param sites The connectivity of the website graph, keyed on unique
     *              website IDs.
     * @param ranks The current ranks of each website, keyed on unique website
     *              IDs.
     * @return The new ranks of the websites graph, using the PageRank
     *         algorithm to update site ranks.
     */
    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {

        System.out.println("Websites Example: ");
        for (Tuple2<Integer, Website> websiteSample: sites.take(5)){
            System.out.println(websiteSample._1() + " -> " + websiteSample._2());
        }

        System.out.println("Ranks Example: ");
        for (Tuple2<Integer, Double> rankSample: ranks.take(5)){
            System.out.println(rankSample._1() + " -> " + rankSample._2());
        }
        System.out.println("Websites Joined on Ranks: ");
        JavaPairRDD<Integer, Tuple2<Website, Double>> join_sites_ranks = sites.join(ranks);
         for (Tuple2<Integer, Tuple2<Website, Double>> joinSample: join_sites_ranks.take(5)){
             System.out.println(joinSample._1() + " -> " + joinSample._2());
         }

         JavaPairRDD<Integer, Double> results = join_sites_ranks.flatMapToPair(key_value ->{
            Website currWebsite = key_value._2()._1();
            Double currWebsiteRank = key_value._2()._2();
            Iterator<Integer> neighborWebs = currWebsite.edgeIterator();
            List<Tuple2<Integer, Double>> newRanks = new ArrayList<>();
//            System.out.println("Curr node (website) neighbors: " + currWebsite.getNEdges());
//            Integer index = 0;
            while(neighborWebs.hasNext()){
//                System.out.println("Neighbor number: " + index);
//                index += 1;
                Integer neighborWebID = neighborWebs.next();
                Tuple2<Integer, Double> rankToNeighbor = new Tuple2(neighborWebID, currWebsiteRank/currWebsite.getNEdges());
                newRanks.add(rankToNeighbor);
            }
            return newRanks.iterator();
         });

         return results;

//        throw new UnsupportedOperationException();
    }
}
