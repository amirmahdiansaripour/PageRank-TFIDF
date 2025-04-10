package edu.coursera.distributed;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;
import java.io.FileNotFoundException;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.List;

public class TF_IDF {
    private String text_file_path;

    TF_IDF(String file_path){
        text_file_path = file_path;
    }

//    TF_IDF(String text_file_path) throws FileNotFoundException{
//        File textFile = new File(text_file_path);
//        Scanner scanner = new Scanner(textFile);
//        while(scanner.hasNextLine()){
//
//        }
//
//    }

    public void termFrequency(JavaSparkContext context){
//        System.out.println("Hello beginning of TF");
      JavaRDD<String> wholeFile  = context.textFile(this.text_file_path);
      List<String> sentences = wholeFile.collect();
      for (String sentence: sentences)
          System.out.println(sentence);

//      JavaRDD<String> words = wholeFile.flatMap(Arrays.asList(file -> file.split(" ")).iterator());
//      JavaPairRDD<Integer, String> wordCounts_initial = words.mapToPair(word -> new Tuple2<>(1, word));
//      JavaPairRDD<Integer, String> wordCounts = wordCounts_initial.reduceByKey((x, y) -> (x + y));
//      return wordCounts;
    }

    public void TF_IDF_algorithm(JavaSparkContext context){
//       JavaPairRDD<Integer, String> wordCounts = termFrequency(context);
//       List<Tuple2<Integer, String>> pairs  = wordCounts.collect();
//       for(Tuple2<Integer, String> kv: pairs)
//           System.out.println("Key: " + kv._1() + " Value: " + kv._2());
        termFrequency(context);
    }

}
