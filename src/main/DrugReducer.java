package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for pain pills analysis with big data methods in the USA and other countries.
 * */

public class DrugReducer extends Reducer<Text, DrugWritable, Text, Text>
{
	public void reduce(Text key, Iterable<DrugWritable> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String, List<Long>> cities = generateHashMap(values);
		ReducerType reducerType = context.getConfiguration().getEnum("reducerType", ReducerType.Sum);

		switch (reducerType)
		{
			case Sum:
				sumReducer(key, context, cities);
				break;
			case Average:
				averageReducer(key, context, cities);
				break;
			case Median:
				medianReducer(key, context, cities);
				break;
			case Min:
				minReducer(key, context, cities);
				break;
			case Max:
				maxReducer(key, context, cities);
				break;
			case Std:
				stdReducer(key, context, cities);
				break;
		}
	}
	
	private HashMap<String, List<Long>> generateHashMap(Iterable<DrugWritable> values)
	{
		HashMap<String, List<Long>> cities = new HashMap<String, List<Long>>();
		
		for (DrugWritable value: values)
		{
			if (!cities.containsKey(value.getCity()))
				cities.put(value.getCity(), new ArrayList<Long>());

			cities.get(value.getCity()).add(value.getUnits());
		}
		
		return cities;
	}

	private void sumReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			long sum = getSum(city.getValue());
	
			context.write(key, new Text(city.getKey() + ": SUM -> " + sum));
		}
	}
	
	private void averageReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			double avg = getAverage(city.getValue());

			context.write(key, new Text(city.getKey() + ": AVG -> " + avg));
		}
	}
	
	private void medianReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			int midPoint = (city.getValue().size() - 1) / 2;
			long median = city.getValue().get(midPoint);

			context.write(key, new Text(city.getKey() + ": MED -> " + median));
		}
	}
	
	private void minReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			long min = getMin(city.getValue());

			context.write(key, new Text(city.getKey() + ": MIN -> " + min));
		}
	}
	
	private void maxReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			long max = getMax(city.getValue());		

			context.write(key, new Text(city.getKey() + ": MAX -> " + max));
		}
	}
	
	private void stdReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			double std = 0;
	
			if (city.getValue().size() > 1)
			{
				double avg = getAverage(city.getValue());
				double var = 0;
				
				for (long val: city.getValue())
					var += Math.pow(val - avg, 2);
				
				var /= (city.getValue().size() - 1);
				std = Math.sqrt(var);
			}

			context.write(key, new Text(city.getKey() + ": STD -> " + std));
		}
	}
	
	private long getSum(List<Long> arr)
	{
		long sum = 0;
		
		for (long val: arr)
			sum += val;
		
		return sum;
	}
	
	private double getAverage(List<Long> arr)
	{
		long sum = getSum(arr);
		return (double)sum / (double)arr.size();
	}
	
	private long getMax(List<Long> arr)
	{
		if (arr.size() == 0)
			return 0;
		
		long max = arr.get(0);
		
		for (int i = 1; i < arr.size(); ++i)
			if (arr.get(i) > max)
				max = arr.get(i);
		
		return max;
	}
	
	private long getMin(List<Long> arr)
	{
		if (arr.size() == 0)
			return 0;
		
		long min = arr.get(0);
		
		for (int i = 1; i < arr.size(); ++i)
			if (arr.get(i) < min)
				min = arr.get(i);
		
		return min;
	}
}