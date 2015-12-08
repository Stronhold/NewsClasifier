
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
        HashMap<String, ArrayList<Tuple2<String, Double>>> diccionarioPalabrasTotales = new HashMap<>();
        HashMap<String, ArrayList<Tuple2<String, Double>>> diccionarioFinal = new HashMap<>();

        List<String> palabras = new ArrayList<>();
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
                JavaPairRDD<String, Double> wordCount = words.mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String s) throws Exception {
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
                            return new Tuple2<String, Double>(s, 1.0);
                        } else {
                            return new Tuple2<String, Double>("", 1.0);
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
                .reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double integer, Double integer2) throws Exception {
                        return integer + integer2;
                    }
                });
                int numeroTotalPalabras = (int) wordCount.count();
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
        System.out.println("Size " + palabras.size());
        for(String key : diccionarioPalabrasTotales.keySet()){
            ArrayList<Tuple2<String, Double>> listadoPalabras = new ArrayList<>();
            ArrayList<Tuple2<String, Double>>  listadoExistente = (ArrayList<Tuple2<String, Double>>) diccionarioPalabrasTotales.get(key);
            Collections.sort(listadoExistente, (o1, o2) -> (o1._2.compareTo(o2._2))*-1);
            ArrayList<Tuple2<String, Double>> listadoRecortado = new ArrayList<>();
            for(int i = 0; i < 20; i++){
                listadoRecortado.add(listadoExistente.get(i));
            }
            for(String word : palabras){
                boolean found = false;

                for(Tuple2<String, Double> t : listadoRecortado){
                    if(t._1.equals(word)){
                        found = true;
                        listadoPalabras.add(new Tuple2<>(t._1, t._2/listadoExistente.size()));
                        break;
                    }
                }
                if(!found){
                    listadoPalabras.add(new Tuple2<>(word, 0.0));
                }

            }
            diccionarioFinal.put(key, listadoPalabras);
        }
        for(String key : diccionarioFinal.keySet()){
            ArrayList<Tuple2<String, Double>> aFreq = diccionarioFinal.get(key);
            System.out.println("Categoría: " + key);
            for(Tuple2<String, Double> t : aFreq){
                System.out.println(t._1 + " " + t._2);
            }
            System.out.println("-------------------------------------------------------------");
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
