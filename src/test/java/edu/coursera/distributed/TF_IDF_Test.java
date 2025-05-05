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

    public List<HashMap<String, Double>> calcGroundTruthforTF(List<String> contents){
        List<HashMap<String, Double>> res = new ArrayList<>();
        for (int i = 0; i < contents.size(); i++) {
            String words[] = contents.get(i).toLowerCase(Locale.ROOT).split(" ");
            res.add(calcWordCountPerFile(words));
        }
        return res;
    }

    public HashMap<String, Set<Integer>> handleWordFreqInDocs(String word, Integer fileIndex, HashMap<String, Set<Integer>> fileIndicesPerWord){
        if(!fileIndicesPerWord.containsKey(word)) {
            Set<Integer> newHashMap = new HashSet<>();
            newHashMap.add(fileIndex);
            fileIndicesPerWord.put(word, newHashMap);
        }

        else{
            if(!fileIndicesPerWord.get(word).contains(fileIndex)) {
                Set<Integer> newWordHash = fileIndicesPerWord.get(word);
                newWordHash.add(fileIndex);
                fileIndicesPerWord.replace(word, newWordHash);
            }
        }
        return fileIndicesPerWord;
    }

    public HashMap<String, Double> calcGroundTruthforIDF(List<String> contents){
        HashMap<String, Set<Integer>> fileIndicesPerWord = new HashMap<>();
        Integer numOfFiles = contents.size();
        for(int fileIndex = 0; fileIndex < contents.size(); fileIndex++){
            String[] words = contents.get(fileIndex).split(" ");
            for(String word : words) {
                fileIndicesPerWord = handleWordFreqInDocs(word, fileIndex, fileIndicesPerWord);
            }
        }
        HashMap<String, Double> IDF_res = new HashMap<>();
        for(String word : fileIndicesPerWord.keySet()) {
            IDF_res.put(word, Math.log((double) numOfFiles / fileIndicesPerWord.get(word).size()));
//            System.out.println(word + " : " + IDF_res.get(word));
        }
        return IDF_res;
    }

    @Test
    public void checkTFLengths(List<HashMap<String, Double>> gt, List<Tuple2<String, Tuple2<Integer, Double>>> testRes){
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
    public void testTF_IDF(){
        String directoryPath = "sample_files";
        TF_IDF test = new TF_IDF(directoryPath);
        test.setFileContents();
        Context context = new Context("Single");
        List<String> fileContents = test.getFileContents();
        JavaPairRDD<Integer, String> textFileRDD = test.generateTextFileRDD(context.getJavaSparkContext());
//        for(Tuple2<Integer, String> text : textFileRDD.collect())
//            System.out.println(text._1() + " : " + text._2());

        JavaPairRDD<String, Tuple2<Integer, Double>> TF_Pairs= test.calcTF(textFileRDD);
//        JavaPairRDD<String, Double> IDF_Pairs = test.calcIDF(TF_Pairs, fileContents.size());
//        for(Tuple2<String, Tuple2<Integer, Double>> onePair : TF_Pairs.collect()){
//            System.out.println("TF_score of word " + onePair._1() + " is " + onePair._2()._2() + " in File " + onePair._2()._1());
//        }
        List<Tuple2<String, Tuple2<Integer, Double>>> toBeComparedTF = TF_Pairs.collect();
        context.stop();
        List<HashMap<String, Double>> groundTruthTF = calcGroundTruthforTF(fileContents);
        HashMap<String, Double> groundTruthIDF = calcGroundTruthforIDF(fileContents);
        checkTFLengths(groundTruthTF, toBeComparedTF);
        checkTFValues(groundTruthTF, toBeComparedTF);
//        checkIDFValues();
    }
}
