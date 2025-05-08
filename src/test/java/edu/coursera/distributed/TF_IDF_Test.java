package edu.coursera.distributed;


import junit.framework.TestCase;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;
import org.junit.runners.Parameterized;
import scala.Int;
import scala.Tuple2;
import scala.reflect.internal.pickling.UnPickler;
import java.io.File;
import java.util.*;
import java.io.FileNotFoundException;

import org.apache.spark.api.java.JavaRDD;

public class TF_IDF_Test extends TestCase {

    private Context context;
    private String directoryPath;
    private TF_IDF test;
    private JavaPairRDD<String, Tuple2<Integer, Double>> TF_pairs;
    private JavaPairRDD<String, Double> IDF_pairs;
    private JavaPairRDD<Integer, String> textFileRDD;
    private List<String> filesContents;

    @Before
    public void setUp(){
        context = new Context("Parallel");
        directoryPath = "sample_files";
        test = new TF_IDF(directoryPath);
        test.setFileContents();
        textFileRDD = test.generateTextFileRDD(context.getJavaSparkContext());
        filesContents = test.getFileContents();
    }

    @After
    public void tearDown(){
        context.stop();
    }

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

    public List<HashMap<String, Double>> calcGroundTruthforTF(){
        List<HashMap<String, Double>> res = new ArrayList<>();
        for (int i = 0; i < filesContents.size(); i++) {
            String words[] = filesContents.get(i).toLowerCase(Locale.ROOT).split(" ");
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

    public HashMap<String, Double> calcGroundTruthforIDF(){
        HashMap<String, Set<Integer>> fileIndicesPerWord = new HashMap<>();
        Integer numOfFiles = filesContents.size();
        for(int fileIndex = 0; fileIndex < filesContents.size(); fileIndex++){
            String[] words = filesContents.get(fileIndex).split(" ");
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

    public HashMap<String, Double> calcGroundTruthforTFIDF(){
        List<HashMap<String, Double>> TF_gt = calcGroundTruthforTF();
        HashMap<String, Double> IDF_gt = calcGroundTruthforIDF();
        HashMap<String, Double> gtRes = new HashMap<>();
        Integer fileID = 0;
        for(HashMap<String, Double> TF_recordHashMap : TF_gt){
            for(String keyWord : TF_recordHashMap.keySet()){
                String keyForTFIDFHash = keyWord + ":" + Integer.toString(fileID);
                gtRes.put(keyForTFIDFHash, TF_recordHashMap.get(keyWord) * IDF_gt.get(keyWord));
            }
            fileID += 1;
        }
        return gtRes;
    }

    @Test
    public void testTFPairsLengths(){
        TF_pairs= test.calcTF(textFileRDD);
        List<HashMap<String, Double>> groundTruthTF = calcGroundTruthforTF();

        int numberOfFiles = groundTruthTF.size();
        Integer wordsPerFileCounter[] = new Integer[numberOfFiles];
        for(int i = 0; i < wordsPerFileCounter.length; i++)
            wordsPerFileCounter[i] = 0;
        Integer maxFileID = 0;
        for(Tuple2<String, Tuple2<Integer, Double>> oneFileRes : TF_pairs.collect()){
            int fileID = oneFileRes._2()._1();
            wordsPerFileCounter[fileID] = wordsPerFileCounter[fileID] + 1;
            if (fileID > maxFileID)
                maxFileID = fileID;
        }
        assertTrue("Number of corpuses falsely detected", maxFileID + 1 == numberOfFiles);

        for(int i = 0; i < groundTruthTF.size(); i++)
            assertTrue("Number of words (records) in corpus " + i + " falsely detected.", wordsPerFileCounter[i] == groundTruthTF.get(i).size());
    }

    @Test
    public void testTFPairsValues(){
        TF_pairs= test.calcTF(textFileRDD);
        List<HashMap<String, Double>> groundTruthTF = calcGroundTruthforTF();
        List<Tuple2<String, Tuple2<Integer, Double>>> TF_Res_list = TF_pairs.collect();
        context.stop();

        for(Tuple2<String, Tuple2<Integer, Double>> oneFileRes : TF_Res_list){
            String wordToBeChecked = oneFileRes._1();
            Integer fileID = oneFileRes._2()._1();
            Double wordTFPerDoc = oneFileRes._2()._2();
            Double correctTF = groundTruthTF.get(fileID).get(wordToBeChecked);
            assertTrue("TF value for word " + wordToBeChecked + " in file " + fileID + " is not correct." + wordTFPerDoc + " != " + correctTF, wordTFPerDoc.equals(correctTF));
        }
    }

    @Test
    public void testIDFPairsLenghts(){
        TF_pairs= test.calcTF(textFileRDD);
        IDF_pairs = test.calcIDF(TF_pairs, filesContents.size());
        HashMap<String, Double> groundTruthIDF = calcGroundTruthforIDF();
        Integer gtNumPairs = groundTruthIDF.keySet().size();
        Integer testNumPairs = IDF_pairs.collect().size();
        assertTrue("Number of IDF pairs not correctly detected by the algorithm",  gtNumPairs.equals(testNumPairs));
        context.stop();
    }

    @Test
    public void testIDFPairsValues(){
        TF_pairs= test.calcTF(textFileRDD);
        IDF_pairs = test.calcIDF(TF_pairs, filesContents.size());
        HashMap<String, Double> groundTruthIDF = calcGroundTruthforIDF();

        for(Tuple2<String, Double> testPair : IDF_pairs.collect()){
            String word = testPair._1();
            Double IDF_valTest = testPair._2();
            Double IDF_gtVal = groundTruthIDF.get(word);
            assertTrue("IDF value for word " + word + " is not calculated correctly. (GT, testVal) = (" + IDF_gtVal + ", " + IDF_valTest + " )"
                    , IDF_gtVal.equals(IDF_valTest));

        }
    }


    @Test
    public void testIDFPairValues(){
        TF_pairs = test.calcTF(textFileRDD);
        IDF_pairs = test.calcIDF(TF_pairs, filesContents.size());
        JavaPairRDD<String, Tuple2<Integer, Double>> TF_IDF_pairs = test.calcTF_IDF_Join(TF_pairs, IDF_pairs);

        List<Tuple2<String, Tuple2<Integer, Double>>> TF_IDF_List = TF_IDF_pairs.collect();
        HashMap<String, Double> groundTruthforTFIDF = calcGroundTruthforTFIDF();

        for(Tuple2<String, Tuple2<Integer, Double>> TF_IDF_record : TF_IDF_List){
            String word = TF_IDF_record._1();
            Integer fileID = TF_IDF_record._2()._1();
            Double TF_IDF_test = TF_IDF_record._2()._2();
            String keyOfGroundTruth = word + ":" + Integer.toString(fileID);
            Double TF_IDF_gt = groundTruthforTFIDF.get(keyOfGroundTruth);
            String errorMessage = "TF-IDF value for word " + word + " in document sample" + (fileID + 1) + ".txt is not correct. (GT, testVal) = (" + TF_IDF_gt + "," + TF_IDF_test +").";
            assertTrue(errorMessage, TF_IDF_gt.equals(TF_IDF_test));
        }
    }

}
