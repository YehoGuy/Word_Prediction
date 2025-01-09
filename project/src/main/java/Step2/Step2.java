package Step2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import Helpers.Consts;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import Step1.Key1;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Step2 {

	public static class Mapper2 extends Mapper<Key1, LongWritable, Key1, LongWritable> {
		@Override
		public void map(Key1 key, LongWritable value, Context context) throws IOException, InterruptedException {
			context.write(key,value);
		}

	}

	public static class Partitioner2 extends Partitioner<Key1, LongWritable> {
		@Override
		public int getPartition(Key1 key, LongWritable value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}


	public static class Reducer2 extends Reducer<Key1,LongWritable,Key2,DoubleWritable> {
		private long currentC2 = 1; // number of times (W1,W2,*) occurs
		@Override
		public void reduce(Key1 key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			LongWritable value = values.iterator().next();
			System.out.println("[DEBUG] Processing line: " + key.toString()+" "  +value.toString());

			try {
				switch (key.size()) {
					case 1:
						context.write(new Key2(key), new DoubleWritable(value.get()));
						break;
					case 2:
						if(value.get() != 0){
							currentC2 = value.get();
							context.write(new Key2(key), new DoubleWritable(value.get()));
						}
						break;
					case 3:
						double N3 = value.get(); // Count of the trigram
						double k3 = (Math.log10(N3+1)+1) / (Math.log10(N3+1)+2); // Calculate k3
						DoubleWritable newVal = new DoubleWritable(k3*(N3/currentC2)); // first addition of the equation.
						context.write(new Key2(key, k3), newVal);
						break;
					default:
						System.err.println("[ERROR] Malformed key: Incorrect word count - " + key.toString());
						break;
				}

			} catch (NumberFormatException e) {
				System.err.println("[ERROR] Error when writing: "+ key.toString()+" "  +value.toString()+ ":   "+e.toString());
			}
		}

	}


	public static void main(String[] args) throws Exception {
		System.out.println("[DEBUG] STEP 2 started!");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Step2: perform K3*(N3/C2) and reorganize");
		job.setJarByClass(Step2.class);
		job.setMapperClass(Mapper2.class);
		job.setPartitionerClass(Partitioner2.class);
		job.setReducerClass(Reducer2.class);

		job.setMapOutputKeyClass(Key1.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Key2.class);
		job.setOutputValueClass(DoubleWritable.class);

//      job.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class); //should only use for last step
		FileInputFormat.setInputPaths(job, new Path(Consts.Step1_Output));
		FileOutputFormat.setOutputPath(job, new Path(Consts.Step2_Output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}



}
