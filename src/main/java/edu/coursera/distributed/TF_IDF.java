package edu.coursera.distributed;

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

   JavaPairRDD<String, Tuple2<Integer, Double>> TF_IDF_algorithm(JavaPairRDD<Integer, String> fileContents){
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

}
