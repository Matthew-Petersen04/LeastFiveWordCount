import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountLeastFive {

    public static class LFMapper
            extends Mapper<Object, Text, Text, LongWritable> {

        private TreeMap<String, Long> tmap;

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {

            /*
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
             */
            tmap = new TreeMap<String, Long>();
        }

        /*
        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }
        */

        @Override
        public void map(Object key, Text value,
                        Context context) throws IOException,
                InterruptedException {

            //input data is a "sentence" of words separated by spaces
            String[] tokens = value.toString().split(" ");

            //goes through the words and adds them to the tmap
            //if the word is not contained in tmap set its value to 1
            //if it does exist take the value, add 1 to it, and update it in tmap

            for (String word : tokens) {
                if (!tmap.containsKey(word)) {
                    tmap.put(word, (long) 1);
                } else {
                    long update = tmap.get(word) + 1;
                    tmap.put(word, update);
                }
            }

            // we remove the first key-value
            // if it's size increases 10
            //if (tmap.size() > 10) {
            //    tmap.remove(tmap.firstKey());
            //}
        }

        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Map.Entry<String, Long> entry : tmap.entrySet()) {

                long count = entry.getValue();
                String word = entry.getKey();

                context.write(new Text(word), new LongWritable(count));
            }
        }
    }

    public static class LFReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        //private LongWritable result = new LongWritable();
        private TreeMap<Long, ArrayList<String>> tmap2;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            tmap2 = new TreeMap<Long, ArrayList<String>>();
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context) throws IOException, InterruptedException {

            // input data from mapper
            // key                values
            // movie_name         [ count ]
            String word = key.toString();
            long count = 0;

            for (LongWritable val : values) {
                count += val.get();
            }

            // insert data into treeMap,
            // we want top 10 viewed movies
            // so we pass count as key
            //tmap2.put(count, name);

            if (!tmap2.containsKey(count)) {
                ArrayList<String> toAdd = new ArrayList<String>();
                tmap2.put(count, new ArrayList<String>());
            }

            //add to the arraylist all words that have the same count
            tmap2.get(count).add(word);


            // we remove the first key-value
            // if it's size increases 5 (for least 5)
            if (tmap2.size() > 5) {
                tmap2.remove(tmap2.lastKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            int n = 5; //least 5 n cannot be in the method

            for (Map.Entry<Long, ArrayList<String>> entry : tmap2.entrySet()) {
                long count = entry.getKey();
                ArrayList<String> words = entry.getValue();
                for (int i = 0; i < words.size(); i++) {
                    if (n > 0) {
                        String word = words.get(i);
                        context.write(new Text(word), new LongWritable(count));
                        n--;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2) {

            // if arguments are not provided
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Least Five Word Count");
        job.setJarByClass(WordCountLeastFive.class);

        job.setMapperClass(LFMapper.class);
        job.setReducerClass(LFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
    }
