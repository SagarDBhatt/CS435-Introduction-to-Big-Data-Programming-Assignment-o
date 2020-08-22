import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class SbhattPA2B {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job4 = new Job(conf);
        job4.setJarByClass(SbhattPA2B.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(20);
        job4.setReducerClass(FinalReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job4, new Path(args[0]),
                TextInputFormat.class, Mapper4.class);
        MultipleInputs.addInputPath(job4, new Path(args[1]),
                TextInputFormat.class, MapperArticle.class);

        FileOutputFormat.setOutputPath(job4, new Path(args[2]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }


    public static class Mapper4 extends Mapper<Object, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valOut = new Text();

        private String title = "DocIDTFfIdf";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String val = value.toString();

            if (!val.isEmpty()) {
                String[] splitter = val.split("\t", 3);

                keyOut.set(splitter[0]); // docId
                valOut.set(title + splitter[1] + "\t" + splitter[2]); // unigram + tfidf

                context.write(keyOut, valOut);
            }
        }
    }


    // Output the articles by docIds
    public static class MapperArticle extends Mapper<Object, Text, Text, Text> {

        private Text DocumentID = new Text();
        private Text resultArticle = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String docID, summary;
            String[] document;

            if (!value.toString().isEmpty()) {
                document = value.toString().split("<====>", 3);
                docID = document[1];
                summary = document[2];

                if (!summary.isEmpty()) {
                    DocumentID.set(docID);
                    resultArticle.set(summary);
                    context.write(DocumentID, resultArticle);
                }
            }
        }

    }


    // Parse the values against each docId to generate summaries. Differentiate by the String prefix. Reduce Side join
    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {

        private Text keyOutDocID = new Text();
        private Text valOutSummary = new Text();

        private String title = "DocIDTFfIdf";

        private HashMap<String, String> tfIdfMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String article = "";
            String documentID = key.toString();
            tfIdfMap = new HashMap<>();

            String val;

            for (Text t : values) {
                val = t.toString();
                if (!val.startsWith(title)) {
                    article = val;
                } else {
                    val = val.substring(title.length());
                    String[] split = new String[2];
                    split = val.split("\t", 2);
                    tfIdfMap.put(split[0], split[1]); // unigram , tfidf
                }
            }

            String[] sentences = article.split("\\.\\s");

//            StringTokenizer tokenizer = new StringTokenizer(article, ". ");

            Double[] sentenceScore = new Double[sentences.length];

            int i = 0;
            while (i < sentences.length) {
                String sentence = sentences[i];
                sentenceScore[i] = scoreSentence(sentence);
                i++;
            }

            String summary = scoreDocument(sentences, sentenceScore);
            keyOutDocID.set(documentID);
            valOutSummary.set(summary + "\n");

            context.write(keyOutDocID, valOutSummary);
        }

        private double scoreSentence(String sentence) {
            StringTokenizer tokenizer = new StringTokenizer(sentence);
            Map<Double, HashMap<String, String>> top5Tree = new TreeMap<>(Collections.<Double>reverseOrder());

            while (tokenizer.hasMoreTokens()) {
                String unigram = tokenizer.nextToken();
                unigram = unigram.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                if (tfIdfMap != null && tfIdfMap.size() > 0) {
                    if (!unigram.trim().isEmpty()) {
                        if (tfIdfMap.get(unigram) != null) {
                            if (top5Tree.containsKey(Double.parseDouble(tfIdfMap.get(unigram)))) {
                                HashMap<String, String> top5TempMap = top5Tree.get(Double.parseDouble(tfIdfMap.get(unigram)));
                                top5TempMap.put(unigram, "");
                                top5Tree.put(Double.parseDouble(tfIdfMap.get(unigram)), top5TempMap);
                            } else {
                                HashMap<String, String> tempMap = new HashMap<>();
                                tempMap.put(unigram, "");
                                top5Tree.put(Double.parseDouble(tfIdfMap.get(unigram)), tempMap);
                            }
                        }
                    }
                }
            }

            Iterator it = top5Tree.entrySet().iterator();
            int count = 5;
            double tfIdfSentence = 0.0;
            while (it.hasNext() && count > 0) {
                Map.Entry pair = (Map.Entry) it.next();
                double tfIdf = (double) pair.getKey();
                Object obj = pair.getValue();
                HashMap<String, String> wordIdfMap = (HashMap<String, String>) obj;

                Iterator itWords = wordIdfMap.entrySet().iterator();
                while (itWords.hasNext()) {
                    itWords.next();
                    tfIdfSentence += tfIdf;
                    count--;
                }

            }
            return tfIdfSentence;
        }

        private String scoreDocument(String[] sentences, Double[] sentenceScore) {
            List<Double> scoreList = Arrays.asList(sentenceScore);
            List<Double> scoreLs = new ArrayList<>();
            scoreLs.addAll(scoreList);

            scoreLs.sort(Collections.reverseOrder());
            String summary = "";
            int[] indices = new int[3];
            int index;

            for (int x = 0; x < 3; x++) {
                indices[x] = -999;
            }

            if (scoreLs.size() > 3) {
                for (int i = 0; i < 3; i++) {
                    Double score = scoreLs.get(i);
                    index = scoreList.indexOf(score);
                    if (index != -1) {
                        indices[i] = index;
                        scoreList.set(index, -999.999);
                    }
                }
            } else {
                for (int i = 0; i < scoreLs.size(); i++) {
                    index = scoreList.indexOf(scoreLs.get(i));
                    if (index != -1) {
                        indices[i] = index;
                        scoreList.set(index, -999.999);
                    }
                }
            }

            Arrays.sort(indices);
            for (int i = 0; i < indices.length; i++) {
                if (indices[i] != -999) {
                    summary += sentences[indices[i]] + ". ";
                }
            }

            return summary;
        }
    }
}