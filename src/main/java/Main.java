import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * Created by Sergio on 02/12/2015.
 */
public class Main {

    public static void main(String [] args){
        //Path donde estar�n las categor�as
        String pathCategories = "src/main/resources/categories/";
        //Configuraci�n b�sica de la aplicaci�n
        SparkConf sparkConf = new SparkConf().setAppName("NaiveBayes").setMaster("local[*]");
        //Creaci�n del contexto
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //Cogemos el fichero en el que se encuentran las categor�as
        File f = new File(pathCategories);
        //Listamos las categor�as
        String [] categories = f.list();
        //Mostramos las categor�as :)
        for(String c : categories){
            System.out.println(c);
            try {


                JavaRDD<String> input = jsc.textFile(pathCategories + c + "/*");
                JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                });
                //TODO hacer filtrado de palabras!
                JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        s = s.trim().toLowerCase();
                        s = s.replace(".", "");
                        s = s.replace("\"", "");
                        s = s.replace(",", "");
                        s = s.replace(":", "");
                        s = s.replace(";", "");
                        s = s.replace("\'", "");
                        s = s.replace("(", "");
                        s = s.replace(")", "");
                        s = s.replace("{", "");
                        s = s.replace("}", "");
                        s = s.replace("-", "");
                        s = s.replace("_", "");
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                });
                List<Tuple2<String, Integer>> total = wordCount.collect();
                Collections.sort(total, (o1, o2) -> (o1._2.compareTo(o2._2))*-1);
                for(Tuple2<String, Integer> t : total){
                    System.out.println(t._1 + " " + t._2);
                }
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
        jsc.stop();
    }
}
