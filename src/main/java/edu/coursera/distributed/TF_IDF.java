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


    public JavaPairRDD<String, Integer> termFrequency(JavaSparkContext context){
      JavaRDD<String> wholeFile  = context.textFile(this.text_file_path);
//      List<String> sentences = wholeFile.collect();
      JavaRDD<String> words = wholeFile.flatMap(file -> Arrays.asList(file.split(" ")).iterator());
//      List<String> words_collected = words.collect();
//      for(String word:words_collected)
//          System.out.println(word);
      JavaPairRDD<String, Integer> wordCounts_initial = words.mapToPair(word -> new Tuple2<>(word, 1));

      JavaPairRDD<String, Integer> wordCounts = wordCounts_initial.reduceByKey((x, y) -> (x + y));
//      List<Tuple2<String, Integer>> words_collected = wordCounts.collect();
      return wordCounts;
    }

    public List<Tuple2<String, Integer>> TF_IDF_algorithm(JavaSparkContext context){
       JavaPairRDD<String, Integer> wordCounts = termFrequency(context);
       List<Tuple2<String, Integer>> pairs  = wordCounts.collect();
//       for(Tuple2<String, Integer> kv: pairs)
//           System.out.println(kv._1() + " " + kv._2());
        return pairs;
    }

}
