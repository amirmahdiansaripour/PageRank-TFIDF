package edu.coursera.distributed;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Context {
    private static JavaSparkContext ctx;
    private static int nCores;

    public static int getNCores() {
        String ncoresStr = System.getenv("COURSERA_GRADER_NCORES");
        if (ncoresStr == null) {
            ncoresStr = System.getProperty("COURSERA_GRADER_NCORES");
        }
        if (ncoresStr == null) {
            return Runtime.getRuntime().availableProcessors();
        }
        else {
            return Integer.parseInt(ncoresStr);
        }
    }

    public Context (final String nCoresString) {
        if(nCoresString == "Single"){
            nCores = 1;
        }
        else if (nCoresString == "Parallel"){
            nCores = getNCores();
        }
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        final SparkConf conf = new SparkConf()
                .setAppName("edu.coursera.distributed.PageRank")
                .setMaster("local[" + nCores + "]")
                .set("spark.ui.showConsoleProgress", "false");
        ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("OFF");
    }


    public JavaSparkContext getJavaSparkContext(){
        return ctx;
    }

    public void stop(){
        ctx.stop();
    }

    public int getNumberofCorses(){
        return nCores;
    }
}