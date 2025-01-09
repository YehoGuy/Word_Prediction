package Step5;

import Helpers.Consts;
import Step4.Key4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Step5 {
	public static class Mapper5 extends Mapper<Key4, DoubleWritable, Key4, DoubleWritable> {
		@Override
		public void map(Key4 key, DoubleWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}


	public static class Partitioner5 extends Partitioner<Key4, DoubleWritable> {
		@Override
		public int getPartition(Key4 key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}



	public static void main(String[] args) throws Exception {
		System.out.println("[DEBUG] STEP 5 started!");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Step3: perform W1 --> W2 --> -probability sorting");
		job.setJarByClass(Step5.class);
		job.setMapperClass(Mapper5.class);
		job.setPartitionerClass(Partitioner5.class);

		job.setMapOutputKeyClass(Key4.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Key4.class);
		job.setOutputValueClass(DoubleWritable.class);

//      job.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); //should only use for last step
		FileInputFormat.setInputPaths(job, new Path(Consts.Step4_Output));
		FileOutputFormat.setOutputPath(job, new Path(Consts.Step5_Output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
