import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.language.bm.Rule;
import org.apache.commons.math3.geometry.Space;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;


public class Pa3_PageRank {

    private static  final Pattern SPACE = Pattern.compile(" ");

    public static void main (String args[]) {

        if (args.length < 1) {
            System.err.println("Error");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Pagerank_PA3")
                .config("spark.master","local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        /*JavaPairRDD<String,String > words = lines.mapToPair(k -> {
            String[] wordSplit = k.split(":");
            return new Tuple2<>(wordSplit[0], wordSplit[1].trim());
        }).distinct().cache();*/

        /*JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(":")).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s,1));

        JavaPairRDD<String, Double>  rank = ones.mapValues(ra -> 1.0);*/

        JavaPairRDD<String, String> words = lines.mapToPair(k -> {
            String[] wordSplit = k.split(":");
            return new Tuple2<>(wordSplit[0], wordSplit[1].trim());
        }).distinct().cache();

        JavaPairRDD<String, Double> rank = words.mapValues(rk -> 1.0);

        for (int x = 0; x < 25; x++) {
            JavaPairRDD<String, Double> contributions = words.join(rank).values().flatMapToPair(k -> {
                String[] wordsComp = k._1().split("\\s");
                int lkTotal = wordsComp.length;
                List<Tuple2<String, Double>> connect = new ArrayList<>();
                for (String i : wordsComp) {
                    connect.add(new Tuple2<>(i, k._2() / lkTotal));
                }
                return connect.iterator();
            });
            rank = contributions.reduceByKey(new AdditionTotal()).mapValues(rc -> rc);

            System.out.println(rank);
        }

        JavaRDD<String>  title = spark.read().textFile(args[1]).javaRDD().cache();

        JavaPairRDD<String, String> titleID = title.zipWithIndex().mapToPair(n -> new Tuple2<>(String.valueOf((n._2() + 1)), n._1()));

        rank.coalesce(1);
        titleID.coalesce(1);

        AtomicReference<String> temp = new AtomicReference<>("");

        JavaPairRDD<String, Double> finalRank = rank;

        JavaPairRDD<String,String> titleWithID = finalRank.join(titleID).values().mapToPair(n -> { return new Tuple2<>(n._1().toString(), n._2()); });

        JavaPairRDD<String, String> sortVal = titleWithID.coalesce(1).sortByKey(false);

        List<Tuple2<String,String>> jvm = sortVal.take(10);

        //System.out.println("Test "+titleWithID);
        /*System.out.println(sortVal);
        sortVal.saveAsTextFile(args[2]);*/

        for(Tuple2<String, String> s : jvm)
        {
            System.out.println(s._1() + " - " + s._2()) ;
        }


        spark.stop();
    }

    public static class AdditionTotal implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double firstvariable, Double secondvariable) {
            return firstvariable + secondvariable;
        }
    }
}
