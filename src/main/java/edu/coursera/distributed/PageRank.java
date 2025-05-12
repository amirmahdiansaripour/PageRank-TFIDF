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
    private PageRank() {
    }

    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {
        Integer NUM_OF_SAMPLES = 3;

//        System.out.println("Websites example: ");
//        for (Tuple2<Integer, Website> websiteSample: sites.take(NUM_OF_SAMPLES)){
//            System.out.println(websiteSample._1() + " -> " + websiteSample._2());
//        }

//        System.out.println("Ranks example: ");
//        for (Tuple2<Integer, Double> rankSample: ranks.take(NUM_OF_SAMPLES)){
//            System.out.println(rankSample._1() + " -> " + rankSample._2());
//        }
//        System.out.println("Websites joined on ranks: ");
        JavaPairRDD<Integer, Tuple2<Website, Double>> join_sites_ranks = sites.join(ranks);
//         for (Tuple2<Integer, Tuple2<Website, Double>> joinSample: join_sites_ranks.take(NUM_OF_SAMPLES)){
//             System.out.println(joinSample._1() + " -> " + joinSample._2());
//         }

         JavaPairRDD<Integer, Double> newRanks = join_sites_ranks.flatMapToPair(key_value ->{
            Website currWebsite = key_value._2()._1();
            Double currWebsiteRank = key_value._2()._2();
            Iterator<Integer> neighborWebs = currWebsite.edgeIterator();
            List<Tuple2<Integer, Double>> newRanksRes = new ArrayList<>();
//            System.out.println("Curr node (website) neighbors: " + currWebsite.getNEdges());
//            Integer index = 0;
            while(neighborWebs.hasNext()){
//                System.out.println("Neighbor number: " + index);
//                index += 1;
                Integer neighborWebID = neighborWebs.next();
                Tuple2<Integer, Double> rankToNeighbor = new Tuple2(neighborWebID, currWebsiteRank/currWebsite.getNEdges());
                newRanksRes.add(rankToNeighbor);
            }
            return newRanksRes.iterator();
         });

//         for(Tuple2<Integer, Double> rank : newRanks.take(NUM_OF_SAMPLES)){
//             System.out.println("Web ID: " + rank._1() + " initial rank: " + rank._2());
//         }

         JavaPairRDD<Integer, Double> summationOfRanks = newRanks.reduceByKey((r1, r2)->(r1 + r2));

//        for(Tuple2<Integer, Double> sum_rank : summationOfRanks.take(NUM_OF_SAMPLES)){
//            System.out.println("Web ID: " + sum_rank._1() +" sum rank: " + sum_rank._2());
//        }

        JavaPairRDD<Integer, Double> weightedFinalRanks = summationOfRanks.mapValues(v -> (0.15 + 0.85 * v));

         return weightedFinalRanks;
    }
}
