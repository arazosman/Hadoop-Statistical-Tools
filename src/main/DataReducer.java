package main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for the Hadoop Project.
 * */

public class DataReducer extends Reducer<Text, DoubleWritable, Text, Text>
{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	{
		String reducerType = context.getConfiguration().get("reducerType");
		Text outputValue;

		switch (reducerType)
		{
			case "sum":
				outputValue = new Text(String.valueOf(getSum(values)));
				break;
			case "min":
				outputValue = new Text(String.valueOf(getMin(values)));
				break;
			case "max":
				outputValue = new Text(String.valueOf(getMax(values)));
				break;
			case "avg":
				outputValue = new Text(String.valueOf(getAverage(values)));
				break;
			case "med":
				outputValue = new Text(String.valueOf(getMedian(values)));
				break;
			case "cnt":
				outputValue = new Text();
				break;
			case "rnd":
				outputValue = new Text();
				break;
			case "var":
				outputValue = new Text();
				break;
			default:
				outputValue = new Text(String.valueOf(getStdDrv(values)));
				break;
		}
		
		context.write(key, outputValue);
	}

	private int getSize(Iterable<DoubleWritable> values)
	{
		int size = 0;

		for (Iterator<DoubleWritable> iterator = values.iterator(); iterator.hasNext(); iterator.next())
			++size;

		return size;
	}

	private double getSum(Iterable<DoubleWritable> values)
	{
		double sum = 0;

		for (DoubleWritable val: values)
			sum += val.get();

		return sum;
	}

	private double getMin(Iterable<DoubleWritable> values)
	{
		double min = Double.MAX_VALUE;

		for (DoubleWritable val: values)
			if (val.get() < min)
				min = val.get();

		return min;
	}

	private double getMax(Iterable<DoubleWritable> values)
	{
		double max = 0;

		for (DoubleWritable val: values)
			if (val.get() > max)
				max = val.get();

		return max;
	}

	private double getAverage(Iterable<DoubleWritable> values)
	{
		double avg = getSum(values) / getSize(values);

		return avg;
	}

	private double getMedian(Iterable<DoubleWritable> values)
	{
		int counter = 0, size = getSize(values), edge = (size - 1) / 2;
		double median = 0;

		Iterator<DoubleWritable> iterator = values.iterator();

		while (counter <= edge && iterator.hasNext())
		{
			median = iterator.next().get();
			++counter;
		}

		if (size % 2 == 0)
			median = (median + iterator.next().get()) / 2;

		return median;
	}

	private double getStdDrv(Iterable<DoubleWritable> values)
	{
		int size = getSize(values);

		if (size < 2)
			return 0;

		double avg = getAverage(values);
		double var = 0;

		for (DoubleWritable val: values)
			var += Math.pow(val.get() - avg, 2);

		var /= (size - 1);
		return Math.sqrt(var);
	}
}