package edu.coursera.distributed;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;

import java.io.File;
import java.util.*;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.nio.file.*;


public class TF_IDF {
    private String directoryPath;
    private Path filesPath;
    private List<String> fileContents;

    TF_IDF(String directory){
        directoryPath = directory;
    }

    public String readFile(String filePath){
        try{
            String res;
            File file = new File(filePath);
            Scanner scanner = new Scanner(file);
            res = scanner.nextLine();
            while (scanner.hasNextLine())
                res += scanner.nextLine();
            scanner.close();
            return res;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return "";
        }
    }

    public void setFileContents(){
        try {
            filesPath = Paths.get(directoryPath);
            DirectoryStream<Path> stream = Files.newDirectoryStream(filesPath);
            fileContents = new ArrayList<String>();
            for (Path path : stream) {
                String pathName = path.getFileName().toString();
                String fileContent = readFile(directoryPath + "\\" + pathName);
                fileContents.add(fileContent);
            }
        } catch(java.io.IOException exc){
            exc.printStackTrace();
        }
    }

    public List<String> getFileContents(){
        return fileContents;
    }

    public JavaPairRDD<Integer, String> generateTextFileRDD(JavaSparkContext context){
        final List<String> localFileContents = new ArrayList<>(fileContents);
        List<Integer> tempArray = new ArrayList<Integer>();
        for(int i = 0; i < fileContents.size(); i++)
            tempArray.add(i);
        JavaPairRDD<Integer, String> textFileRDD = context.parallelize(tempArray).mapToPair(i -> new Tuple2<>(i, localFileContents.get(i)));
        return textFileRDD;
    }

    public JavaPairRDD<String, Double> calcIDF(JavaPairRDD<String, Tuple2<Integer, Double>> input, Integer numOfFiles){
        JavaPairRDD<String, Integer> wordsInDocs = input.mapToPair(pair ->{
           return new Tuple2<String, Integer>(pair._1(), pair._2()._1());
        });
        JavaPairRDD<String, Iterable<Integer>> groupedByWord = wordsInDocs.distinct().groupByKey();
//        List<Tuple2<String, Iterable<Integer>>> groupedByWordShow = groupedByWord.collect();
//        for(Tuple2<String, Iterable<Integer>> pair: groupedByWordShow){
//            System.out.println(pair._1() + " : " + pair._2().toString());
//        }
        JavaPairRDD<String, Double> wordCountInWholeFiles = groupedByWord.mapToPair(pair ->{
            Integer totalNumOfFilesWordAppeard = Iterables.size(pair._2());
            String word = pair._1();
            Double IDF_Score = Math.log((double) numOfFiles / totalNumOfFilesWordAppeard);
            return new Tuple2<>(word, IDF_Score);
        }
        );
        return wordCountInWholeFiles;
    }

   public JavaPairRDD<String, Tuple2<Integer, Double>> calcTF(JavaPairRDD<Integer, String> fileContents){
       JavaPairRDD<String, Tuple2<Integer, Double>> TF_Res = fileContents.flatMapToPair(file ->{
            Integer fileID = file._1();
            String[] textWords = file._2().toLowerCase(Locale.ROOT).split(" ");
            Integer numofWords = textWords.length;
            List<Tuple2<String, Tuple2<Integer, Double>>> correctFormForTF = new ArrayList<>();
            HashMap<String, Integer> wordCounts = new HashMap<>();
            for(String word : textWords){
                if(!wordCounts.containsKey(word))
                    wordCounts.put(word, 1);
                else
                    wordCounts.replace(word, wordCounts.get(word) + 1);
            }
            for(String key : wordCounts.keySet()){
                correctFormForTF.add(new Tuple2<>(key, new Tuple2<>(fileID, (double) wordCounts.get(key)/numofWords)));
            }
            return correctFormForTF.iterator();
        });
        return TF_Res;
   }

   public JavaPairRDD<String, Tuple2<Integer, Double>> calcTF_IDF_Join(JavaPairRDD<String, Tuple2<Integer, Double>> TF_RDD,
                                                      JavaPairRDD<String, Double> IDF_RDD){

        JavaPairRDD<String, Tuple2<Tuple2<Integer, Double>, Double>> Joined_TF_on_IDF = TF_RDD.join(IDF_RDD);

        JavaPairRDD<String, Tuple2<Integer, Double>> TF_IDF_finalRes = Joined_TF_on_IDF.mapToPair(joinedRecord -> {
            String word = joinedRecord._1();
            Integer textID = joinedRecord._2()._1()._1();
            Double TF_score = joinedRecord._2()._1()._2();
            Double IDF_score = joinedRecord._2()._2();
            Double TF_IDF_multiplied = TF_score * IDF_score;
            Tuple2<Integer, Double> secondComponent = new Tuple2<>(textID, TF_IDF_multiplied);
            return new Tuple2<>(word, secondComponent);
        }
        );

        return TF_IDF_finalRes;
   }

}
