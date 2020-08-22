import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import scala.Tuple2;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;

public final class Pa3_Wikibomb {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("WikiBomb_PA3")
                .config("spark.master","local")
                .getOrCreate();

        JavaPairRDD<String, String> ID = spark.read().textFile(args[1]).javaRDD().zipWithIndex().mapToPair(i -> new Tuple2<>(String.valueOf((i._2() + 1)), i._1()));

        Dataset<Row> tableVal = spark.createDataset(ID.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("ID", "titles");

        tableVal.createOrReplaceTempView("titles");

        JavaPairRDD<String, String> links = spark.read().textFile(args[0]).javaRDD().mapToPair(k -> {
            String[] wordSplit = k.split(":");
            return new Tuple2<>(wordSplit[0], wordSplit[1].trim());
        });
        Dataset<Row> tableLinks = spark.createDataset(links.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("ID", "link");

        tableLinks.createOrReplaceTempView("tableOfLink");

        JavaRDD<Row> initWG = spark.sql("SELECT ID as ID, link as link FROM tableOfLink WHERE ID IN (Select ID FROM titles WHERE UPPER(titles) LIKE UPPER('%surfing%'))").toJavaRDD().repartition(16);

        JavaPairRDD<String, String> titleID = spark.read().textFile(args[1]).javaRDD().zipWithIndex().mapToPair(i -> new Tuple2<>(i._1(), String.valueOf((i._2() + 1)))).repartition(16);

        List<String> contID = titleID.lookup(args[0]);
        String contn = "";

        if (contID != null && contID.size() > 0) {
            contn = " " + contID.get(0);
        }

        final String contnF = contn;

        JavaPairRDD<String, String> weight = initWG.mapToPair(k -> new Tuple2<>(k.getString(0), k.getString(1) + contnF + contnF + contnF + contnF + contnF));
        JavaPairRDD<String, Double> rank = weight.mapValues(rs -> 1.0);

        for (int x = 0; x < 25; x++) {
            JavaPairRDD<String, Double> contribution = weight.join(rank).values().flatMapToPair(k -> {
                String[] outLink = k._1().split("\\s");
                int totalOutLinks = outLink.length;
                List<Tuple2<String, Double>> connectss = new ArrayList<>();
                for (String i : outLink) {
                    connectss.add(new Tuple2<>(i, k._2() / totalOutLinks));
                }
                return connectss.iterator();
            });
            rank = contribution.reduceByKey(new AdditionTotal()).mapValues(vi -> 0.15 + vi * 0.85);
        }
        rank.coalesce(1);
        ID.coalesce(1);

        //AtomicReference<String> sortVal = new AtomicReference<>("");

        /*rank.mapToPair(k -> new Tuple2<>(k._2(), k._1())).sortByKey(false).take(10).forEach(k -> {
                    List<String> ttl = titlesID.lookup(k._2);
                    if (ttl != null && ttl.size() > 0) {
                        //System.out.println(k._2() + "," + ttl.get(0) + "," + k._1());
                        sortVal.set(k._2() + "," + ttl.get(0) + "," + k._1());
                    }
                }
        );

        System.out.println(sortVal);
        */

        AtomicReference<String> temp = new AtomicReference<>("");

        JavaPairRDD<String, Double> finalRank = rank;

        JavaPairRDD<String,String> titleWithID = finalRank.join(ID).values().mapToPair(n -> { return new Tuple2<>(n._1().toString(), n._2()); });

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
    private static class AdditionTotal implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double firstvariable, Double secondvariable) {
            return firstvariable + secondvariable;
        }
    }
}