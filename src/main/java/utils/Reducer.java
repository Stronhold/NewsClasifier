package utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Sergio on 08/12/2015.
 */
public class Reducer {
    public static JavaPairRDD<String, Double> parseWords(JavaRDD<String> words, HashMap<String, String> dictionary) {
        return words.mapToPair(new PairFunction<String, String, Double>() {
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
    }

    public static HashMap<String, ArrayList<Tuple2<String, Double>>> getDictionaryWithLastWords(HashMap<String, ArrayList<Tuple2<String, Double>>> diccionarioPalabrasTotales, HashMap<String, ArrayList<Tuple2<String, Double>>> diccionarioFinal, List<String> palabras) {
        for(String key : diccionarioPalabrasTotales.keySet()){
            ArrayList<Tuple2<String, Double>> listadoPalabras = new ArrayList<>();
            ArrayList<Tuple2<String, Double>>  listadoExistente = (ArrayList<Tuple2<String, Double>>) diccionarioPalabrasTotales.get(key);
            Collections.sort(listadoExistente, (o1, o2) -> (o1._2.compareTo(o2._2)) * -1);
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
        return diccionarioFinal;
    }
}
