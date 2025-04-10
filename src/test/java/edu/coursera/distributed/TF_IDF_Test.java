package edu.coursera.distributed;

import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

public class TF_IDF_Test extends TestCase {

    @Test
    public void testTermFrequency(){
        TF_IDF test = new TF_IDF("sample.txt");
        Context context = new Context("Single");
        test.TF_IDF_algorithm(context.getJavaSparkContext());
        context.stop();
    }

}
