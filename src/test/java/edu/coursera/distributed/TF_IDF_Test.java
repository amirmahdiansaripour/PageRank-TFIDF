package edu.coursera.distributed;

import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Int;
import scala.Tuple2;
import scala.reflect.internal.pickling.UnPickler;
import java.io.File;
import java.util.*;
import java.io.FileNotFoundException;

import org.apache.spark.api.java.JavaRDD;

public class TF_IDF_Test extends TestCase {

    public HashMap<String, Double> calcWordCountPerFile(String[] words){
        HashMap<String, Double> res = new HashMap<>();
        for (String word : words) {
            if (!res.containsKey(word))
                res.put(word, 1.0);
            else
                res.replace(word, res.get(word) + 1);
        }
        for(String key: res.keySet())
            res.replace(key, res.get(key) / words.length);
        return res;
    }

    public List<HashMap<String, Double>> calcGroundTruth(List<String> contents){
        List<HashMap<String, Double>> res = new ArrayList<>();
        for (int i = 0; i < contents.size(); i++) {
            String words[] = contents.get(i).toLowerCase(Locale.ROOT).split(" ");
            res.add(calcWordCountPerFile(words));
        }
        return res;
    }

    @Test
    public void checkLengths(List<HashMap<String, Double>> gt, List<Tuple2<String, Tuple2<Integer, Double>>> testRes){
        int numberOfFiles = gt.size();
        Integer wordsPerFileCounter[] = new Integer[numberOfFiles];
        for(int i = 0; i < wordsPerFileCounter.length; i++)
            wordsPerFileCounter[i] = 0;
        Integer maxFileID = 0;
        for(Tuple2<String, Tuple2<Integer, Double>> oneFileRes : testRes){
            int fileID = oneFileRes._2()._1();
            wordsPerFileCounter[fileID] = wordsPerFileCounter[fileID] + 1;
            if (fileID > maxFileID)
                maxFileID = fileID;
        }
        assertTrue("Number of corpuses falsely detected", maxFileID + 1 == numberOfFiles);

        for(int i = 0; i < gt.size(); i++)
            assertTrue("Number of words (records) in corpus " + i + " falsely detected.", wordsPerFileCounter[i] == gt.get(i).size());

    }

    @Test
    public void checkTFValues(List<HashMap<String, Double>> gt, List<Tuple2<String, Tuple2<Integer, Double>>> testRes){
        for(Tuple2<String, Tuple2<Integer, Double>> oneFileRes : testRes){
            String wordToBeChecked = oneFileRes._1();
            Integer fileID = oneFileRes._2()._1();
            Double wordTFPerDoc = oneFileRes._2()._2();
            Double correctTF = gt.get(fileID).get(wordToBeChecked);
            assertTrue("TF value for word " + wordToBeChecked + " in file " + fileID + " is not correct." + wordTFPerDoc + " != " + correctTF, wordTFPerDoc.equals(correctTF));
        }
    }


    @Test
    public void testTermFrequency(){
        String directoryPath = "sample_files";
        TF_IDF test = new TF_IDF(directoryPath);
        test.setFileContents();
        Context context = new Context("Single");
        JavaPairRDD<Integer, String> textFileRDD = test.generateTextFileRDD(context.getJavaSparkContext());
//        for(Tuple2<Integer, String> text : textFileRDD.collect())
//            System.out.println(text._1() + " : " + text._2());

        JavaPairRDD<String, Tuple2<Integer, Double>> TF_Pairs= test.TF_IDF_algorithm(textFileRDD);
//        for(Tuple2<String, Tuple2<Integer, Double>> onePair : TF_Pairs.collect()){
//            System.out.println("TF_score of word " + onePair._1() + " is " + onePair._2()._2() + " in File " + onePair._2()._1());
//        }
        List<Tuple2<String, Tuple2<Integer, Double>>> toBeCompared = TF_Pairs.collect();
        context.stop();
        List<String> fileContents = test.getFileContents();
        List<HashMap<String, Double>> groundTruth = calcGroundTruth(fileContents);
        checkLengths(groundTruth, toBeCompared);
        checkTFValues(groundTruth, toBeCompared);
    }

}
