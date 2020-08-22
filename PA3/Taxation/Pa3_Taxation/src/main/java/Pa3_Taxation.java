import java.util.List;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;


public class Pa3_Taxation {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("Pagerank_Taxation")
                .config("spark.master","local")
                .getOrCreate();


        JavaRDD<String> varlk = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, String> rddVarlk = varlk.mapToPair(k -> {
            String[] wordSplit = k.split(":");
            return new Tuple2<>(wordSplit[0], wordSplit[1].trim());
        }).distinct().cache();

        JavaPairRDD<String, Double> rank = rddVarlk.mapValues(rk -> 1.0);

        for (int x = 0; x < 25; x++) {
            JavaPairRDD<String, Double> contributions = rddVarlk.join(rank).values().flatMapToPair(k -> {
                String[] varlkcomp = k._1().split("\\s");
                int lkTotal = varlkcomp.length;
                List<Tuple2<String, Double>> connect = new ArrayList<>();
                for (String i : varlkcomp) {
                    connect.add(new Tuple2<>(i, k._2() / lkTotal));
                }
                return connect.iterator();
            });

            rank = contributions.reduceByKey(new AdditionTotal()).mapValues(rc -> 0.15 + rc * 0.85);
        }

        JavaPairRDD<String, String> titleID = spark.read().textFile(args[1]).javaRDD().zipWithIndex()
                .mapToPair(n -> new Tuple2<>(String.valueOf((n._2() + 1)), n._1()));

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

    }
    private static class AdditionTotal implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double firstvariable, Double secondvariable) {
            return firstvariable + secondvariable;
        }
    }
}