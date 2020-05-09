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
			case MinMax:
				minMaxReducer(key, context, cities);
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
			long sum = 0;
			
			for (long val: city.getValue())
				sum += val;
	
			context.write(key, new Text(city.getKey() + ": " + sum));
		}
	}
	
	private void averageReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			long sum = 0;
			
			for (long val: city.getValue())
				sum += val;

			double avg = (double)sum / (double)city.getValue().size();

			context.write(key, new Text(city.getKey() + ": " + avg));
		}
	}
	
	private void medianReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			int midPoint = (city.getValue().size() - 1) / 2;
			long median = city.getValue().get(midPoint);

			context.write(key, new Text(city.getKey() + ": " + median));
		}
	}
	
	private void minMaxReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{
			long min = Collections.min(city.getValue());
			long max = Collections.max(city.getValue());
			
			context.write(key, new Text(city.getKey() + ": " + min + " - " + max));
		}
	}
	
	private void stdReducer(Text key, Context context, HashMap<String, List<Long>> cities) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> city: cities.entrySet())
		{

		}
	}
}