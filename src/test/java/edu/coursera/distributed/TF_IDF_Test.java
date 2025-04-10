package edu.coursera.distributed;

import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Int;
import scala.Tuple2;
import scala.reflect.internal.pickling.UnPickler;
import java.io.File;
import java.util.HashMap;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.util.List;

public class TF_IDF_Test extends TestCase {

    public HashMap<String, Integer> getTermFrequency(String filePath) throws FileNotFoundException{
        File file = new File(filePath);
        Scanner scanner = new Scanner(file);
        HashMap<String, Integer> wordCountFreq = new HashMap<String, Integer>();
        while(scanner.hasNextLine()){
            String[] words = scanner.nextLine().split(" ");
            for(String word : words){
                if (!wordCountFreq.containsKey(word))
                    wordCountFreq.put(word, 1);
                else
                    wordCountFreq.put(word, wordCountFreq.get(word) + 1);
            }
        }
//        System.out.println("Key values of a Hashmap");
//        for(String word:wordCountFreq.keySet()){
//            System.out.println(word + " : " + wordCountFreq.get(word));
//        }
        return wordCountFreq;
    }

    @Test
    public void testTermFrequency() throws FileNotFoundException{
        String filePath = "sample.txt";
        TF_IDF test = new TF_IDF(filePath);
        Context context = new Context("Single");
        List<Tuple2<String, Integer>> TF_IDF_Res = test.TF_IDF_algorithm(context.getJavaSparkContext());
        context.stop();
        HashMap<String, Integer> groundTruth = getTermFrequency(filePath);
//        System.out.println("wordFreq size:\t\t" + wordFreqs.size());
//        System.out.println("correctRes size:\t" + correctRes.size());
        assertTrue("The number of words detected by TF-IDF is not correct",TF_IDF_Res.size() == groundTruth.size());
        for(Tuple2<String, Integer> wordPair : TF_IDF_Res){
            Integer TF_res = wordPair._2();
            Integer correctFreq = groundTruth.get(wordPair._1());
            assertTrue("Frequency of the word " + wordPair._1() + " is not detected correctly by TF " + TF_res + " != " + correctFreq , TF_res.equals(correctFreq));
        }

    }

}
