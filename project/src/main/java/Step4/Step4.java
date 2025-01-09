package Step4;

import Helpers.Consts;
import Step2.Key2;
import Step3.Key3;
import Step3.Step3;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Step4 {
	public static class Mapper4 extends Mapper<Key3, DoubleWritable, Key3, DoubleWritable> {
		@Override
		public void map(Key3 key, DoubleWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}


	public static class Partitioner4 extends Partitioner<Key3, DoubleWritable> {
		@Override
		public int getPartition(Key3 key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}


	public static class Reducer4 extends Reducer<Key3,DoubleWritable, Key4, DoubleWritable> {
		double currentN1 = 1; //number of time (w3,*,*) occurs
		double C0 = 1; //number of time (*,*,*) occurs
		@Override
		public void setup(Context output) throws IOException {
			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
			S3Object s3Object = s3.getObject(new GetObjectRequest(Consts.BUCKET, Consts.C0_Key));
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
			C0 = Long.parseLong(reader.readLine());
		}
		@Override
		public void reduce(Key3 key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
			DoubleWritable value = values.iterator().next();
			System.out.println("[DEBUG] Processing line: " + key.toString()+" "  +value.toString());

			try {
				switch (key.size()) {
					case 1:
						currentN1 = value.get();
						break;
					case 2:
					case 3:
						double k3 = key.getK3();
						double k2 = key.getK2();
						double updatedVal = value.get() + ((1-k3)*(1-k2)*(currentN1/C0));
						context.write(new Key4(key,updatedVal), new DoubleWritable(updatedVal));
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
		System.out.println("[DEBUG] STEP 4 started!");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Step3: perform (1-k3)*(1-k2)*(N1/C0) and reorganize");
		job.setJarByClass(Step4.class);
		job.setMapperClass(Mapper4.class);
		job.setPartitionerClass(Partitioner4.class);
		job.setReducerClass(Reducer4.class);

		job.setMapOutputKeyClass(Key3.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Key4.class);
		job.setOutputValueClass(DoubleWritable.class);

//      job.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class); //should only use for last step
		FileInputFormat.setInputPaths(job, new Path(Consts.Step3_Output));
		FileOutputFormat.setOutputPath(job, new Path(Consts.Step4_Output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
