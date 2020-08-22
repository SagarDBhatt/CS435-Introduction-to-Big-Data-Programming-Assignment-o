import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class SbhattPA2A {

    public static class GlobalCounter {
        public static enum Counter {
            glbCounter
        }
    }

    // Write <docId, unigram>
    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        public Text keyOut = new Text();
        public Text valOut = new Text();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String summary, docID;
            String[] articles;

            if(!value.toString().isEmpty()){

                articles = value.toString().split("<====>", 3);
                summary = articles[2];
                docID = articles[1];

                //System.out.println(summary);
                //System.out.println(docID);

                if(!summary.isEmpty()){
                    context.getCounter(GlobalCounter.Counter.glbCounter).increment(1);

                    StringTokenizer Tokenizer = new StringTokenizer(summary);

                    while(Tokenizer.hasMoreTokens()){
                        String unigram = Tokenizer.nextToken();
                        unigram = unigram.replaceAll("[^a-zA-Z0-9]]","").toLowerCase();
                        keyOut.set(docID);
                        valOut.set(unigram);

                        if(!unigram.isEmpty())
                        {
                            context.write(keyOut,valOut);
                        }

                    }
                }
            }
        }
    }//End of mapper1

    //For TF value
    /*public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        public Text keyOut = new Text();
        public Text termFreq = new Text();
        String fnKeyout;
        public Map<String,Integer> linkMap = new LinkedHashMap<>();
        public Map<Integer, HashMap<String,String>> resultMap = new TreeMap<>(Collections.reverseOrder());;
        int temp,freq;
        double termFreqUni;

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            linkMap = new LinkedHashMap<>();
            for (Text val : value) {
                if (linkMap.containsKey(val.toString())) {
                    temp = linkMap.get(val.toString());
                    linkMap.put(val.toString(), temp + 1);
                }
            }

            Iterator itrt = linkMap.entrySet().iterator();
            resultMap = new TreeMap<>(Collections.reverseOrder());

            while (itrt.hasNext()) {
                Map.Entry entryPair = (Map.Entry) itrt.next();
                fnKeyout = entryPair.getKey().toString();

                if (resultMap.containsKey((int) entryPair.getValue())) {
                    HashMap<String, String> linkMap = resultMap.get((int) entryPair.getValue());
                    linkMap.put(fnKeyout, key.toString());
                    resultMap.put((int) entryPair.getValue(), linkMap);
                } else {
                    HashMap<String, String> list = new HashMap<>();
                    list.put(fnKeyout, key.toString());
                    resultMap.put((int) entryPair.getValue(), list);
                }

                if (resultMap.size() > 0) {
                    int maxCount = 0;
                    Iterator itrtT = resultMap.entrySet().iterator();

                    while (itrtT.hasNext()) {
                        Map.Entry pair = (Map.Entry) itrtT.next();

                        if (maxCount == 0) {
                            maxCount = (int) pair.getKey();
                        }

                        Object aObject = pair.getValue();
                        HashMap<String, String> allUnigrams = (HashMap<String, String>) aObject;

                        Iterator itrtV = allUnigrams.entrySet().iterator();

                        while (itrtV.hasNext()) {

                            Map.Entry keyValue = (Map.Entry) itrtV.next();
                            freq = (int) pair.getKey();
                            termFreqUni = 0.5 + 0.5 * (((double) freq / (double) maxCount));

                            keyOut.set(key + "\t" + keyValue.getKey().toString());
                            termFreq.set(String.valueOf(termFreqUni));
                            context.write(keyOut, termFreq);
                        }
                    }
                }
            }
        }
    }//End of Reducer1*/

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text termFreq = new Text();
        public String fnKeyout="";
        private Map<String, Integer> linkMap = new LinkedHashMap<>();
        private Map<Integer, HashMap<String, String>> resultMap = new TreeMap<>(Collections.reverseOrder());
        int temp,freq;
        double termFreqUni;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            linkMap = new LinkedHashMap<>();
            for (Text t : values) {
                if (linkMap.containsKey(t.toString())) {
                    temp = linkMap.get(t.toString());
                    linkMap.put(t.toString(), temp + 1);
                } else {
                    linkMap.put(t.toString(), 1);
                }
            }

            Iterator it = linkMap.entrySet().iterator();

            resultMap = new TreeMap<>(Collections.reverseOrder());

            while (it.hasNext()) {
                Map.Entry comb = (Map.Entry) it.next();
                fnKeyout = comb.getKey().toString();
                if (resultMap.containsKey((int) comb.getValue())) {
                    HashMap<String, String> linkMap = resultMap.get((int) comb.getValue());
                    linkMap.put(fnKeyout, key.toString());
                    resultMap.put((int) comb.getValue(), linkMap);
                } else {
                    HashMap<String, String> tempList = new HashMap<>();
                    tempList.put(fnKeyout, key.toString());
                    resultMap.put((int) comb.getValue(), tempList);
                }
            }


            if (resultMap.size() > 0) {
                int maximum = 0;
                Iterator itrtT = resultMap.entrySet().iterator();
                while (itrtT.hasNext()) {
                    Map.Entry comb = (Map.Entry) itrtT.next();
                    if (maximum == 0) {
                        maximum = (int) comb.getKey();
                    }

                    Object o = comb.getValue();
                    HashMap<String, String> unigramList = (HashMap<String, String>) o;
                    Iterator itValues = unigramList.entrySet().iterator();
                    while (itValues.hasNext()) {
                        Map.Entry valuesForKey = (Map.Entry) itValues.next();
                        freq = (int) comb.getKey();
                        termFreqUni = 0.5 + 0.5 * (((double) freq / (double) maximum));

                        keyOut.set(key + "\t" + valuesForKey.getKey().toString());
                        termFreq.set(String.valueOf(termFreqUni));
                        context.write(keyOut, termFreq);
                    }
                }
            }
        }
    }



    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valOut = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String docID, summary;
            String[] articles;

            if (!value.toString().isEmpty()) {
                articles = value.toString().split("<====>", 3);
                docID = articles[1];
                summary = articles[2];

                if (!summary.isEmpty()) {
                    StringTokenizer tokenizer = new StringTokenizer(summary);
                    while (tokenizer.hasMoreTokens()) {
                        String currentUnigram = tokenizer.nextToken();
                        currentUnigram = currentUnigram.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (!currentUnigram.trim().isEmpty()) {
                            keyOut.set(currentUnigram);
                            valOut.set(docID);
                            context.write(keyOut, valOut);
                        }
                    }
                }
            }
        }
    }


    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valOut = new Text();
        Set<String> docIdSet = new HashSet<>();
        double idfValue;
        public long count;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            this.count = context.getConfiguration().getLong(GlobalCounter.Counter.glbCounter.name(), 0);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fValue = key.toString();
            docIdSet = new HashSet<>();

            for (Text t : values) {
                docIdSet.add(t.toString());
            }

            idfValue = Math.log10((double) count / (double) docIdSet.size());

            for (String s : docIdSet) {
                keyOut.set(s + "\t" + fValue);
                valOut.set(String.valueOf(idfValue));
                context.write(keyOut, valOut);
            }
        }
    }


    public static class Mapper3TF extends Mapper<Object, Text, Text, Text> {
        private Text keyOutTF = new Text();
        private Text valOutTF = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tfLine;

            if (!value.toString().isEmpty()) {
                tfLine = value.toString().split("\t", 3);
                keyOutTF.set(tfLine[0] + "\t" + tfLine[1]); // docid + "\t" + unigram
                valOutTF.set(/*"TF" + */tfLine[2]); // TF value
                context.write(keyOutTF, valOutTF);
            }

        }
    }


    public static class Mapper3IDF extends Mapper<Object, Text, Text, Text> {

        private Text keyOutIDF = new Text();
        private Text valOutIDF = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] idfLine;

            if (!value.toString().isEmpty()) {
                idfLine = value.toString().split("\t", 3);
                keyOutIDF.set(idfLine[0] + "\t" + idfLine[1]);
                valOutIDF.set(idfLine[2]);
                context.write(keyOutIDF, valOutIDF);
            }
        }
    }


    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

        private Text keyOutDocID = new Text();
        private Text valOutUnig = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String docID = key.toString().split("\t")[0];
            String unigram = key.toString().split("\t")[1];

            double tfidf = 1;

            for (Text t : values) {
                tfidf *= Double.parseDouble(t.toString());
            }

            keyOutDocID.set(docID);
            valOutUnig.set(unigram + "\t" + String.valueOf(tfidf));

            context.write(keyOutDocID, valOutUnig);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job aJobA = new Job();
        aJobA.setJarByClass(SbhattPA2A.class);
        aJobA.setMapperClass(Mapper1.class);
        aJobA.setMapOutputKeyClass(Text.class);
        aJobA.setMapOutputValueClass(Text.class);
        aJobA.setNumReduceTasks(20);
        aJobA.setReducerClass(Reducer1.class);
        aJobA.setOutputKeyClass(Text.class);
        aJobA.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(aJobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(aJobA, new Path(args[1]));
        aJobA.waitForCompletion(true);

        Counter someCount = aJobA.getCounters().findCounter(GlobalCounter.Counter.glbCounter);

        Job aJobB = new Job();
        aJobB.getConfiguration().setLong(GlobalCounter.Counter.glbCounter.name(), someCount.getValue());
        aJobB.setJarByClass(SbhattPA2A.class);
        aJobB.setMapperClass(Mapper2.class);
        aJobB.setMapOutputKeyClass(Text.class);
        aJobB.setMapOutputValueClass(Text.class);
        aJobB.setNumReduceTasks(20);
        aJobB.setReducerClass(Reducer2.class);
        aJobB.setOutputKeyClass(Text.class);
        aJobB.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(aJobB, new Path(args[0]));
        FileOutputFormat.setOutputPath(aJobB, new Path(args[2]));
        aJobB.waitForCompletion(true);

        Job aJobC = new Job();
        aJobC.setJarByClass(SbhattPA2A.class);
        aJobC.setMapOutputKeyClass(Text.class);
        aJobC.setMapOutputValueClass(Text.class);
        aJobC.setNumReduceTasks(20);
        aJobC.setReducerClass(Reducer3.class);
        aJobC.setOutputKeyClass(Text.class);
        aJobC.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(aJobC, new Path(args[1]),
                TextInputFormat.class, Mapper3TF.class);
        MultipleInputs.addInputPath(aJobC, new Path(args[2]),
                TextInputFormat.class, Mapper3IDF.class);

        FileOutputFormat.setOutputPath(aJobC, new Path(args[3]));

        System.exit(aJobC.waitForCompletion(true) ? 0 : 1);
    }//end of main

}//End of class PA2A