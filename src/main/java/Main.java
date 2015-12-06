import com.esotericsoftware.kryo.Kryo;
import edu.upc.freeling.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Sergio on 02/12/2015.
 */
public class Main {

    public static void main(String [] args){

        //Path donde estaran las categorias
        String pathCategories = "src/main/resources/categories/";

        //Configuracion basica de la aplicacion
        SparkConf sparkConf = new SparkConf().setAppName("NaiveBayes").setMaster("local[*]");

        //Creacion del contexto
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //Cogemos el fichero en el que se encuentran las categorias
        File f = new File(pathCategories);
        //Listamos las categorias
        String [] categories = f.list();
        HashMap<String, String> dictionary = loadDictionary();
        //Mostramos las categorias :)
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
                        s = Normalizer.normalize(s, Normalizer.Form.NFD);
                        Pattern pattern = Pattern.compile("\\P{ASCII}+");
                        s = pattern.matcher(s).replaceAll("");
                        if (!dictionary.containsKey(s) && !isNumeric(s)) {
                            return new Tuple2<String, Integer>(s, 1);
                        } else {
                            return new Tuple2<String, Integer>("", 1);
                        }
                    }

                    private boolean isNumeric(String s) {
                        try
                        {
                            double d = Double.parseDouble(s);
                        }
                        catch(NumberFormatException nfe)
                        {
                            return false;
                        }
                        return true;
                    }

                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                });
                List<Tuple2<String, Integer>> total = wordCount.collect();
                Collections.sort(total, (o1, o2) -> (o1._2.compareTo(o2._2))*-1);
                List<Tuple2<String, Integer>> elementsRemoved = new ArrayList<>();
                for(Tuple2<String, Integer> t : total){
                    if(!t._1.equals("")){
                        elementsRemoved.add(t);
                    }
                }
                for(Tuple2<String, Integer> t : elementsRemoved){
                    System.out.println(t._1 + " " + t._2);
                }
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
        jsc.stop();
    }

    private static HashMap<String, String> loadDictionary() {
        HashMap<String, String> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/StopWords.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                map.put(line, line);
            }
        }catch (Exception e){

        }

        return map;
    }
}
