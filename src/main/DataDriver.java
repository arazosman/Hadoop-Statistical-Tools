package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * Driver class for pain pills analysis with big data methods in the USA and some other countries.
 * */

public class DataDriver
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Path input_dir = new Path(files[0]);
		Path output_dir = new Path(files[1]);

		conf.set("targetColumn", files[2]);
		conf.set("byColumns", files[3]);
		conf.set("reducerType", files[4]);

		Job job = Job.getInstance(conf, "PainPills");
		job.setJarByClass(DataDriver.class);

		job.setMapperClass(DataMapper.class);
		job.setReducerClass(DataReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);
		
		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}