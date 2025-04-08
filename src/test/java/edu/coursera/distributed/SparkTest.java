package edu.coursera.distributed;

import scala.Int;
import scala.Tuple2;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import junit.framework.TestCase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest extends TestCase {

    private static enum EdgeDistribution {
        INCREASING,
        RANDOM,
        UNIFORM
    }

    private static Integer NUM_OF_SAMPLES = 3;

    private static JavaSparkContext getSparkContext(final int nCores) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        final SparkConf conf = new SparkConf()
            .setAppName("edu.coursera.distributed.PageRank")
            .setMaster("local[" + nCores + "]")
            .set("spark.ui.showConsoleProgress", "false");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("OFF");
        return ctx;
    }

    private static int getNCores() {
        String ncoresStr = System.getenv("COURSERA_GRADER_NCORES");
        if (ncoresStr == null) {
            ncoresStr = System.getProperty("COURSERA_GRADER_NCORES");
        }

        if (ncoresStr == null) {
            return Runtime.getRuntime().availableProcessors();
        } else {
            return Integer.parseInt(ncoresStr);
        }
    }

    private static Website generateWebsite(final int i, final int nNodes,
            final int minEdgesPerNode, final int maxEdgesPerNode,
            final EdgeDistribution edgeConfig) {
        Random r = new Random(i);

        Website site = new Website(i);

        final int nEdges;
        switch (edgeConfig) {
            case INCREASING:
                double frac = (double)i / (double)nNodes;
                double offset = (double)(maxEdgesPerNode - minEdgesPerNode)
                    * frac;
                nEdges = minEdgesPerNode + (int)offset;
                break;
            case RANDOM:
                nEdges = minEdgesPerNode +
                    r.nextInt(maxEdgesPerNode - minEdgesPerNode);
                break;
            case UNIFORM:
                nEdges = maxEdgesPerNode;
                break;
            default:
                throw new RuntimeException();
        }

        for (int j = 0; j < nEdges; j++) {
            site.addEdge(r.nextInt(nNodes));
        }

        return site;
    }

    private static JavaPairRDD<Integer, Website> generateGraphRDD(
            final int nNodes, final int minEdgesPerNode,
            final int maxEdgesPerNode, final EdgeDistribution edgeConfig,
            final JavaSparkContext context) {
        List<Integer> nodes = new ArrayList<Integer>(nNodes);
        for (int i = 0; i < nNodes; i++) {
            nodes.add(i);
        }

        JavaPairRDD<Integer, Website> graphRDD = context.parallelize(nodes).mapToPair(
                i -> new Tuple2<>(i, generateWebsite(i, nNodes, minEdgesPerNode, maxEdgesPerNode, edgeConfig))
        );

//        List<Tuple2<Integer, Website>> collectedRdd = graphRDD.collect();
//        for (int j = 0; j < NUM_OF_SAMPLES; j++){
//            System.out.println(collectedRdd.get(j));
//        }
        return graphRDD;
    }

    private static JavaPairRDD<Integer, Double> generateRankRDD(
            final int nNodes, final JavaSparkContext context) {
        List<Integer> nodes = new ArrayList<Integer>(nNodes);
        for (int i = 0; i < nNodes; i++) {
            nodes.add(i);
        }

        JavaPairRDD<Integer, Double> rankRDD = context.parallelize(nodes).mapToPair(
                i -> {
                    Random randomRank = new Random(i);
                    return new Tuple2(i, 100.0 * randomRank.nextDouble());
                }
        );

//        List<Tuple2<Integer, Double>> collectedRdd = rankRDD.collect();
//        for(int t = 0; t < NUM_OF_SAMPLES; t++){
//            System.out.println(collectedRdd.get(t));
//        }

        return rankRDD;
    }

    private static Website[] generateGraphArr(final int nNodes,
            final int minEdgesPerNode, final int maxEdgesPerNode,
            final EdgeDistribution edgeConfig) {
        Website[] sites = new Website[nNodes];
        for (int i = 0; i < sites.length; i++) {
            sites[i] = generateWebsite(i, nNodes, minEdgesPerNode,
                    maxEdgesPerNode, edgeConfig);
        }
        return sites;
    }

    private static double[] generateRankArr(final int nNodes) {
        double[] ranks = new double[nNodes];
        for (int i = 0; i < ranks.length; i++) {
            Random r = new Random(i);
//            System.out.println("R: " + r);
//            System.out.println("Next Double: " + r.nextDouble());
            ranks[i] = 100.0 * r.nextDouble();
        }
        return ranks;
    }

    private static double[] seqPageRank(Website[] sites, double[] ranks) {
        double[] newRanks = new double[ranks.length];

        for (int j = 0; j < sites.length; j++) {
            Iterator<Integer> iter = sites[j].edgeIterator();
            while (iter.hasNext()) {
                int target = iter.next();
                newRanks[target] += ranks[j] / (double)sites[j].getNEdges();
            }
        }

        for (int j = 0; j < newRanks.length; j++) {
            newRanks[j] = 0.15 + 0.85 * newRanks[j];
        }


        return newRanks;
    }

    private static List<Tuple2<Integer, Double>> pageRankAlgorithm(int repeats, JavaSparkContext context, final int nNodes,
                                             final int minEdgesPerNode, final int maxEdgesPerNode, final int niterations,
                                                    EdgeDistribution edgeConfig){
        JavaPairRDD<Integer, Website> nodes = null;
        JavaPairRDD<Integer, Double> ranks = null;
        List<Tuple2<Integer, Double>> parResults = null;

        for (int r = 0; r < repeats; r++) {
//            System.out.println("\n" + "GraphRDD elements example:");
            nodes = generateGraphRDD(nNodes, minEdgesPerNode,
                    maxEdgesPerNode, edgeConfig, context);

//            System.out.println("RankRDD elements example:");
            ranks = generateRankRDD(nNodes, context);

            for (int i = 0; i < niterations; i++) {
                ranks = PageRank.sparkPageRank(nodes, ranks);
            }
        }
        parResults = ranks.collect();
//        System.out.println("Final ranks:");
//        for(int i = 0; i < NUM_OF_SAMPLES; i++){
//            System.out.println("Web ID: " + parResults.get(i)._1() + " Web rank: " + parResults.get(i)._2());
//        }

        return parResults;
    }

    private static void testDriver(final int nNodes, final int minEdgesPerNode,
            final int maxEdgesPerNode, final int niterations,
            final EdgeDistribution edgeConfig) {
        System.out.println("Running the PageRank algorithm for " + niterations +
                " iterations on a website graph of " + nNodes + " websites" + "\n");

        final int repeats = 2;
        Website[] nodesArr = generateGraphArr(nNodes, minEdgesPerNode, maxEdgesPerNode, edgeConfig);
//        System.out.print("Example of a website: " + nodesArr[0] + "\n");

//        System.out.println("Websites ranks example (random initial values):");
        double[] ranksArr = generateRankArr(nNodes);
        for (int i = 0; i < niterations; i++) {
            ranksArr = seqPageRank(nodesArr, ranksArr);
        }

        // Serial case
        JavaSparkContext context = getSparkContext(1);
        final long singleStart = System.currentTimeMillis();
        List<Tuple2<Integer, Double>> singleCoreResult = pageRankAlgorithm(repeats, context, nNodes, minEdgesPerNode, maxEdgesPerNode, niterations, edgeConfig);
        final long singleElapsed = System.currentTimeMillis() - singleStart;
        context.stop();

        // Parallel case
        JavaSparkContext multiCoreContext = getSparkContext(getNCores());
        final long parStart = System.currentTimeMillis();
        List<Tuple2<Integer, Double>> parResult = pageRankAlgorithm(repeats, multiCoreContext, nNodes, minEdgesPerNode, maxEdgesPerNode, niterations, edgeConfig);
        final long parElapsed = System.currentTimeMillis() - parStart;
        multiCoreContext.stop();

        final double speedup = (double)singleElapsed / (double)parElapsed;

        // HashMap is empty
        Map<Integer, Double> keyed = new HashMap<Integer, Double>();
        for (Tuple2<Integer, Double> site : parResult) {
            assert (!keyed.containsKey(site._1()));
            keyed.put(site._1(), site._2());
        }

        // Having web ID in the hash map and the difference between ranks
        assertEquals(nodesArr.length, parResult.size());
        for (int i = 0; i < parResult.size(); i++) {
            assertTrue(keyed.containsKey(nodesArr[i].getId()));
            final double delta = Math.abs(ranksArr[i] - keyed.get(nodesArr[i].getId()));
            assertTrue(delta < 1E-9);
        }

        System.out.println();
        System.out.println("Single-core execution ran in " + singleElapsed + " ms");
        System.out.println(getNCores() + "-core execution ran in " + parElapsed + " ms, yielding a speedup of " + speedup + "x");
        System.out.println();

        final double expectedSpeedup = 1.2;
        final String msg = "Expected at least " + expectedSpeedup + "x speedup, but only saw " + speedup + "x. Sequential time = " +
            singleElapsed + " ms, parallel time = " + parElapsed + " ms";
        assertTrue(msg, speedup >= expectedSpeedup);
    }

    public void testUniformTwentyThousand() {
        final int nNodes = 20000;
        final int minEdgesPerNode = 20;
        final int maxEdgesPerNode = 40;
        final int niterations = 5;
        final EdgeDistribution edgeConfig = EdgeDistribution.UNIFORM;

        testDriver(nNodes, minEdgesPerNode, maxEdgesPerNode, niterations,
                edgeConfig);
    }

    public void testUniformFiftyThousand() {
        final int nNodes = 50000;
        final int minEdgesPerNode = 20;
        final int maxEdgesPerNode = 40;
        final int niterations = 5;
        final EdgeDistribution edgeConfig = EdgeDistribution.UNIFORM;

        testDriver(nNodes, minEdgesPerNode, maxEdgesPerNode, niterations,
                edgeConfig);
    }

    public void testIncreasingTwentyThousand() {
        final int nNodes = 20000;
        final int minEdgesPerNode = 20;
        final int maxEdgesPerNode = 40;
        final int niterations = 5;
        final EdgeDistribution edgeConfig = EdgeDistribution.INCREASING;

        testDriver(nNodes, minEdgesPerNode, maxEdgesPerNode, niterations,
                edgeConfig);
    }

    public void testIncreasingFiftyThousand() {
        final int nNodes = 50000;
        final int minEdgesPerNode = 20;
        final int maxEdgesPerNode = 40;
        final int niterations = 5;
        final EdgeDistribution edgeConfig = EdgeDistribution.INCREASING;

        testDriver(nNodes, minEdgesPerNode, maxEdgesPerNode, niterations,
                edgeConfig);
    }

    public void testRandomTwentyThousand() {
        final int nNodes = 20000;
        final int minEdgesPerNode = 20;
        final int maxEdgesPerNode = 40;
        final int niterations = 5;
        final EdgeDistribution edgeConfig = EdgeDistribution.RANDOM;

        testDriver(nNodes, minEdgesPerNode, maxEdgesPerNode, niterations,
                edgeConfig);
    }

    public void testRandomFiftyThousand() {
        final int nNodes = 50000;
        final int minEdgesPerNode = 20;
        final int maxEdgesPerNode = 40;
        final int niterations = 5;
        final EdgeDistribution edgeConfig = EdgeDistribution.RANDOM;

        testDriver(nNodes, minEdgesPerNode, maxEdgesPerNode, niterations,
                edgeConfig);
    }
}
