package Step3;

import Helpers.Consts;
import Step1.Key1;
import Step2.Key2;
import Step2.Step2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step3 {

	public static class Mapper3 extends Mapper<Key2, DoubleWritable, Key2, DoubleWritable> {
		@Override
		public void map(Key2 key, DoubleWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}


	public static class Partitioner3 extends Partitioner<Key2, DoubleWritable> {
		@Override
		public int getPartition(Key2 key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}


	public static class Reducer3 extends Reducer<Key2,DoubleWritable,Key3, DoubleWritable> {
		double currentC1 = 1; //number of time (w2,*,*) occurs
		double currentN2 = 1; //number of time (w2,w3,*) occurs
		@Override
		public void reduce(Key2 key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
			DoubleWritable value = values.iterator().next();
			System.out.println("[DEBUG] Processing line: " + key.toString()+" "  +value.toString());

			try {
				switch (key.size()) {
					case 1:
						currentC1 = value.get();
						context.write(new Key3(key), new DoubleWritable(value.get()));
						break;
					case 2:
						currentN2 = value.get();
						break;
					case 3:
						double k3 = key.getK3();
						double k2 =(Math.log10(currentN2+1)+1) / (Math.log10(currentN2+1)+2); // Calculate k2
						double updatedVal = value.get() + ((1-k3)*k2*(currentN2/currentC1));
						context.write(new Key3(key, k2), new DoubleWritable(updatedVal));
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
		System.out.println("[DEBUG] STEP 3 started!");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Step3: perform (1-k3)*k2*(N2/C1) and reorganize");
		job.setJarByClass(Step3.class);
		job.setMapperClass(Mapper3.class);
		job.setPartitionerClass(Partitioner3.class);
		job.setReducerClass(Reducer3.class);

		job.setMapOutputKeyClass(Key2.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Key3.class);
		job.setOutputValueClass(DoubleWritable.class);

//      job.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class); //should only use for last step
		FileInputFormat.setInputPaths(job, new Path(Consts.Step2_Output));
		FileOutputFormat.setOutputPath(job, new Path(Consts.Step3_Output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
