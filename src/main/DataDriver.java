package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * Driver class for the Hadoop Project.
 * */

public class DataDriver
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		String[] newArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path input_dir = new Path(newArgs[0]);
		Path output_dir = new Path(newArgs[1]);

		conf.set("targetColumn", newArgs[2]);
		conf.set("dependentColumns", newArgs[3]);
		conf.set("reducerType", newArgs[4]);

		Job job = Job.getInstance(conf, "HadoopProject");

		job.setJarByClass(DataDriver.class);
		job.setMapperClass(DataMapper.class);
		job.setReducerClass(DataReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);

		output_dir.getFileSystem(job.getConfiguration()).delete(output_dir, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}