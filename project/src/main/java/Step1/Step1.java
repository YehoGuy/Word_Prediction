package Step1;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import Helpers.Consts;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * The First Step is Responsible for Counting the Words, pairs & trigrams in the Input File.
 * Outputs a lexicographically ordered (by key) list, where each item is structured as one of:
 * (key = <w1,*,*> , value = Count(w1))         -- single word count
 * (key = <w1,w2,*> , value = Count(w1,w2))     -- pair count
 * (key = <w1,w2,w3> , value = Count(w1,w2,w3)) -- trigram count
 */
public class Step1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Key1, LongWritable> {

        private final String[] badWords = {
                "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ", "למה",
                "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד",
                "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
                "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה",
                "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-",
                "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם",
                "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם",
                "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן",
                "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן",
                "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
        };
        Set<String> badWordSet = new HashSet<>(Arrays.asList(badWords));
        private Key1 writeKey;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("[DEBUG] Processing line: " + value.toString());

            // Split the line into tab-separated fields
            // entrance format is: ngram TAB year TAB match_count TAB volume_count NEWLINE
            String[] valueSplitted = value.toString().split("\t");
            System.out.println(valueSplitted.length);
            for(int i=0 ; i< valueSplitted.length ; i++){
                System.out.println(valueSplitted[i]);
            }

            // Split the first field (ngram) into three words
            String[] ngram = valueSplitted[0].split(" ");
            if (ngram.length != 3) {  // Ensure the ngram contains exactly 3 words
                System.err.println("[ERROR] Malformed ngram:     " + valueSplitted[0]);
                return;
            }

            // ------- filter --------
            for(String word: ngram){
                for(int i=0 ; i< word.length() ; i++){
                    if(word.charAt(i) < 'א' || word.charAt(i) > 'ת'){
                        System.err.println("[SKIPPED] Malformed ngram: Contains non-hebrew characters - " + valueSplitted[0]);
                        return;
                    }
                }
                if(badWordSet.contains(word)){
                    System.err.println("[SKIPPED] Malformed ngram: Contains bad word - " + valueSplitted[0]);
                    return;
                }
            }
            //---------------

            try {
                // Parse match_count as an integer
                LongWritable matchCount = new LongWritable(Long.parseLong(valueSplitted[2]));

                // Extract individual words
                String firstWord = ngram[0];
                String secondWord = ngram[1];
                String thirdWord = ngram[2];

                // Write output key-value pairs
                writeKey = new Key1(Consts.STAR, Consts.STAR, Consts.STAR);
                context.write(writeKey, matchCount);
                writeKey = new Key1(firstWord, Consts.STAR, Consts.STAR);
                context.write(writeKey, matchCount);
                writeKey = new Key1(firstWord, secondWord, Consts.STAR);
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
     * for Count(<*,*,*>), the value, which by the formula is named C0, is written to a global file.
     */
    public static class Reducer1 extends Reducer<Key1,LongWritable,Key1,LongWritable> {
        private void uploadC0(Context context, Long c0) throws IOException {
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
            String sumString = String.valueOf(c0);
            InputStream stream = new ByteArrayInputStream(sumString.getBytes());
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(sumString.getBytes().length);
            PutObjectRequest request = new PutObjectRequest(Consts.BUCKET, Consts.C0_Key, stream ,metadata);
            s3.putObject(request);
        }
        @Override
        public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            if(key.size() == 0)
                 uploadC0(context, sum);
            else
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

        //for running on AWS Emr:
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Consts.Step1_Input));
        FileOutputFormat.setOutputPath(job, new Path(Consts.Step1_Output));

        //for testing locally:
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path("test.txt"));
        //TextOutputFormat.setOutputPath(job, new Path("output_test"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    

}
