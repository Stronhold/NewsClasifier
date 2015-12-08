
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;
import utils.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by Sergio on 02/12/2015.
 */
public class Main {

    public static void main(String [] args){

        //Path de resultados
        String pathResults = "results";
        try {
            FileUtils.deleteDirectory(new File(pathResults));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

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

        //Diccionarios para su uso posterior
        HashMap<String, String> dictionary = loadDictionary();
        HashMap<String, ArrayList<Tuple2<String, Double>>> diccionarioPalabrasTotales = new HashMap<>();
        HashMap<String, ArrayList<Tuple2<String, Double>>> diccionarioFinal = new HashMap<>();

        List<String> palabras = new ArrayList<>();
        //Mostramos las categorias
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
                JavaPairRDD<String, Double> wordCount = Reducer.parseWords(words, dictionary);

                List<Tuple2<String, Double>> total = wordCount.collect();
                List<Tuple2<String, Double>> elementsRemoved = new ArrayList<>();
                for(Tuple2<String, Double> t : total){
                    if(!t._1.equals("")){
                        elementsRemoved.add(t);
                    }
                }
                Collections.sort(elementsRemoved, (o1, o2) -> (o1._2.compareTo(o2._2))*-1);
                ArrayList<String> recortado = new ArrayList<>();
                for(int i = 0; i < 20; i++){
                    recortado.add(elementsRemoved.get(i)._1);
                }
                for(String s : recortado){
                    if(!palabras.contains(s)){
                        palabras.add(s);
                    }
                }
                diccionarioPalabrasTotales.put(c, (ArrayList<Tuple2<String, Double>>) elementsRemoved);

            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }

        //Añadimos las restantes palabras a cada categoría y hacemos su frecuencia
        diccionarioFinal = Reducer.getDictionaryWithLastWords(diccionarioPalabrasTotales, diccionarioFinal, palabras);

        //Creación del modelo
        ArrayList<LabeledPoint> aLabeledPoint = new ArrayList<>();
        double number = 1.0;
        for(String key : diccionarioFinal.keySet()){
            ArrayList<Tuple2<String, Double>> aFreq = diccionarioFinal.get(key);
            double[] v = new double[aFreq.size()];
            for(int i = 0; i < aFreq.size(); i++){
                Tuple2<String, Double> t =  aFreq.get(i);
                v[i] = t._2;
            }
            LabeledPoint l = new LabeledPoint(number, Vectors.dense(v));
            number++;
            aLabeledPoint.add(l);
        }
        JavaRDD<LabeledPoint> labels = jsc.parallelize(aLabeledPoint);
        NaiveBayesModel model = NaiveBayes.train(labels.rdd());
        //Guardamos el modelo
        model.save(jsc.sc(), pathResults);
        jsc.stop();
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

        }

        return map;
    }
}
