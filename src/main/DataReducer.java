package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		List<Double> numbers = toArrayList(values);
		String reducerType = context.getConfiguration().get("reducerType");
		Text outputValue;

		switch (reducerType)
		{
			case "sum":
				outputValue = new Text(String.valueOf(getSum(numbers)));
				break;
			case "min":
				outputValue = new Text(String.valueOf(getMin(numbers)));
				break;
			case "max":
				outputValue = new Text(String.valueOf(getMax(numbers)));
				break;
			case "avg":
				outputValue = new Text(String.valueOf(getAverage(numbers)));
				break;
			case "med":
				outputValue = new Text(String.valueOf(getMedian(numbers)));
				break;
			case "mod":
				outputValue = new Text(String.valueOf(getMode(numbers)));
				break;
			case "cnt":
				outputValue = new Text(String.valueOf(numbers.size()));
				break;
			case "var":
				outputValue = new Text(String.valueOf(getVariance(numbers)));
				break;
			default:
				outputValue = new Text(String.valueOf(getStdDrv(numbers)));
				break;
		}

		context.write(key, outputValue);
	}
	
	private List<Double> toArrayList(Iterable<DoubleWritable> values)
	{
		List<Double> list = new ArrayList<Double>();
		
		for (DoubleWritable val: values)
			list.add(val.get());

		return list;
	}

	private double getSum(List<Double> numbers)
	{
		double sum = 0;

		for (Double val: numbers)
			sum += val;

		return sum;
	}

	private double getMin(List<Double> numbers)
	{
		double min = Double.MAX_VALUE;

		for (double val: numbers)
			if (val < min)
				min = val;

		return min;
	}

	private double getMax(List<Double> numbers)
	{
		double max = 0;

		for (double val: numbers)
			if (val > max)
				max = val;

		return max;
	}

	private double getAverage(List<Double> numbers)
	{
		return getSum(numbers) / numbers.size();
	}

	private double getMedian(List<Double> numbers)
	{
		if (numbers.size() % 2 == 1)
			return numbers.get(numbers.size() / 2);

		return (numbers.get(numbers.size() / 2) + numbers.get((numbers.size() / 2) + 1)) / 2;
	}

	private double getMode(List<Double> numbers)
	{
		HashMap<Double, Integer> map = new HashMap<Double, Integer>();

		for (Double val: numbers)
		{
			if (!map.containsKey(val))
				map.put(val, 1);
			else
				map.replace(val, map.get(val) + 1);
		}

		double mode = 0;
		int maxFreq = 0;

		for (Map.Entry<Double, Integer> number: map.entrySet())
			if (number.getValue() > maxFreq)
			{
				mode = number.getKey();
				maxFreq = number.getValue();
			}

		return mode;
	}

	private double getVariance(List<Double> numbers)
	{
		if (numbers.size() < 2)
			return 0;

		double avg = getAverage(numbers);
		double mse = 0;

		for (double val: numbers)
			mse += Math.pow(val - avg, 2);

		double var = mse / (numbers.size() - 1);

		return var;
	}

	private double getStdDrv(List<Double> numbers)
	{
		double std = Math.sqrt(getVariance(numbers));

		return std;
	}
}