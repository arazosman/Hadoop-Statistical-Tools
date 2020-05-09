package main;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for pain pills analysis with big data methods in the USA and some other countries.
 * */

public class DrugReducer extends Reducer<Text, DrugWritable, Text, Text>
{
	public void reduce(Text key, Iterable<DrugWritable> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities = generateHashMap(values);
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
	
	private HashMap<String, AbstractMap.SimpleEntry<Long, Long>> generateHashMap(Iterable<DrugWritable> values) 
	{
		HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities = new HashMap<String, AbstractMap.SimpleEntry<Long, Long>>();

		for (DrugWritable value: values)
		{
			if (!cities.containsKey(value.getCity()))
				cities.put(value.getCity(), new AbstractMap.SimpleEntry<Long, Long>(value.getUnits(), 1L));
			else
			{
				long units = value.getUnits() + cities.get(value.getCity()).getKey();
				long count = 1 + cities.get(value.getCity()).getValue();
				cities.replace(value.getCity(), new AbstractMap.SimpleEntry<Long, Long>(units, count));
			}
		}

		return cities;		
	}

	private void sumReducer(Text key, Context context, HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities) 
			throws IOException, InterruptedException
	{
		for (Map.Entry<String, AbstractMap.SimpleEntry<Long, Long>> city: cities.entrySet())
		{
			Text value = new Text(city.getKey() + ": " + city.getValue().getKey());
			context.write(key, value);
		}
	}
	
	private void averageReducer(Text key, Context context, HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities) 
			throws IOException, InterruptedException
	{
		for (Map.Entry<String, AbstractMap.SimpleEntry<Long, Long>> city: cities.entrySet())
		{
			double avg = (double)city.getValue().getKey() / (double)city.getValue().getValue();
			Text value = new Text(city.getKey() + ": " + avg);
			context.write(key, value);
		}
	}
	
	private void medianReducer(Text key, Context context, HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities) 
			throws IOException, InterruptedException
	{
	}
	
	private void minMaxReducer(Text key, Context context, HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities) 
			throws IOException, InterruptedException
	{
		
	}
	
	private void stdReducer(Text key, Context context, HashMap<String, AbstractMap.SimpleEntry<Long, Long>> cities) 
			throws IOException, InterruptedException
	{
		
	}
}