import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.*;
import scala.Tuple2;
import utils.Reducer;

import java.io.*;
import java.util.*;

/**
 * Created by Sergio on 08/12/2015.
 */
public class Test {
    public static void main(String [] args){

        //Path de resultados
        String pathResults = "results";


        String pathToWords = "words.txt";
        File file = new File(pathToWords);

        //Path donde estaran las categorias
        String pathCategories = "src/main/resources/categoriestest/";

        //Configuracion basica de la aplicacion
        SparkConf sparkConf = new SparkConf().setAppName("NaiveBayesTest").setMaster("local[*]");

        //Creacion del contexto
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        NaiveBayesModel model = NaiveBayesModel.load(jsc.sc(), pathResults);


        HashMap<String, String> dictionary = loadDictionary();


        JavaRDD<String> fileWords = null;

        if(file.exists()){
            JavaRDD<String> input = jsc.textFile(pathToWords);
            fileWords = input.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            });
        }
        else{
            System.out.println("Error, there is no words");
            System.exit(-1);
        }
        ArrayList<String> aFileWords = (ArrayList<String>) fileWords.collect();

        //Cogemos el fichero en el que se encuentran las categorias
        File dir = new File(pathCategories);
        for(File f : dir.listFiles()){
            JavaRDD<String> input = jsc.textFile(f.getPath());
            JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            });
            JavaPairRDD<String, Double> wordCount = Reducer.parseWords(words, dictionary);
            //TODO el count se hace mal, se tienen que coger las palabras no "", pero bueno, por ahora vale...
            List<Tuple2<String, Double>> total = wordCount.collect();
            List<Tuple2<String, Double>> elementsRemoved = new ArrayList<>();
            for(Tuple2<String, Double> t : total){
                if(!t._1.equals("")){
                        elementsRemoved.add(new Tuple2<>(t._1, t._2/wordCount.count()));
                }
            }
            ArrayList<Tuple2<String, Double>> freqFinal = new ArrayList<>();
            for(String s : aFileWords){
                boolean found = false;
                for(Tuple2<String, Double> t : elementsRemoved){
                    if(t._1.equals(s)){
                        found = true;
                        freqFinal.add(t);
                        break;
                    }
                }
                if(!found){
                    freqFinal.add(new Tuple2<String, Double>(s, 0.0));
                }
            }
            double [] v = new double[freqFinal.size()];
            for(int i = 0; i < freqFinal.size(); i++){
                Tuple2<String, Double> t =  freqFinal.get(i);
                v[i] = t._2;
            }
            org.apache.spark.mllib.linalg.Vector vector = Vectors.dense(v);
            /**/
            double d = model.predict(vector);
            if(d == 1.0){
                System.out.println("Política");
            }
            else if(d == 2.0){
                System.out.println("Tecnología");
            }
            else if(d == 3.0){
                System.out.println("Juegos");
            }
            else if (d == 4.0){
                System.out.println("Economía");
            }
            else{
                System.out.println("Deportes");
            }

        }

        jsc.stop();
    }

    private static void createFile(List<String> palabras, String pathToWords) {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(pathToWords, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        for(String temp : palabras){
            writer.println(temp);
        }
        writer.close();
    }

    /**
     * Cargamos el diccionario
     * @return
     */
    private static HashMap<String, String> loadDictionary() {
        HashMap<String, String> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/StopWords.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                map.put(line, line);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        return map;
    }
}
