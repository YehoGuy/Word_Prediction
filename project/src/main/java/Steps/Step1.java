package Steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import Keys.Key1;
/*
 * The First Step is Responsible for Counting the Words, pairs & trigrams in the Input File.
 * Outputs a lexicographically ordered (by key) list, where each item is structured as one of:
 * (key = <w1,*,*> , value = Count(w1))         -- single word count
 * (key = <w1,w2,*> , value = Count(w1,w2))     -- pair count
 * (key = <w1,w2,w3> , value = Count(w1,w2,w3)) -- trigram count
 */
public class Step1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Key1, LongWritable> {
        Key1 writeKey;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("[DEBUG] Processing line: " + value.toString());

            // Split the line into tab-separated fields
            // entrance format is: ngram TAB year TAB match_count TAB volume_count NEWLINE
            String[] valueSplitted = value.toString().split("-");
            System.out.println(valueSplitted.length);
            for(int i=0 ; i< valueSplitted.length ; i++){
                System.out.println(valueSplitted[i]);
            }

            // Split the first field (ngram) into three words
            String[] ngram = valueSplitted[0].split(" ");
            if (ngram.length != 3) {  // Ensure the ngram contains exactly 3 words
                System.err.println("[ERROR] Malformed ngram: Incorrect word count - " + valueSplitted[0]);
                return;
            }

            //TODO: check that no word is a known bad word

            try {
                // Parse match_count as an integer
                LongWritable matchCount = new LongWritable(Long.parseLong(valueSplitted[2]));

                // Extract individual words
                String firstWord = ngram[0];
                String secondWord = ngram[1];
                String thirdWord = ngram[2];

                // Write output key-value pairs
                writeKey = new Key1("*", "*", "*");
                context.write(writeKey, matchCount);
                writeKey = new Key1(firstWord, "*", "*");
                context.write(writeKey, matchCount);
                writeKey = new Key1(firstWord, secondWord, "*");
                context.write(writeKey, matchCount);
                writeKey = new Key1(firstWord, secondWord, thirdWord);
                context.write(writeKey, matchCount);

            } catch (NumberFormatException e) {
                System.err.println("[ERROR] Error when writing: " + e.toString());
            }
        }

    }
    
    /*
     * the combiner is a local aggregation optimization
     * it is used to reduce the amount of data that is sent to the reducer
     * by reducing locally in the map nodes, before sending the data to the partitioner
     */
    public static class Combiner1 extends Reducer<Key1, LongWritable, Key1, LongWritable> {
        @Override
        public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class Partitioner1 extends Partitioner<Key1, LongWritable> {
        @Override
        public int getPartition(Key1 key, LongWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    /*
     * The reducer sums up the final count for each word, pair and trigram.
     * writes the output in the format: key = <word1 word2 word3> , value = count
     */
    public static class Reducer1 extends Reducer<Key1,LongWritable,Key1,LongWritable> {
        @Override
        public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1: Count");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Mapper1.class);
        job.setPartitionerClass(Partitioner1.class);
        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Key1.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Key1.class);
        job.setOutputValueClass(LongWritable.class);

//      job.setOutputFormatClass(TextOutputFormat.class);

        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileInputFormat.addInputPath(job, new Path("test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output_word_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    

}
