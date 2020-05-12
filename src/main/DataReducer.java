package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for pain pills analysis with big data methods in the USA and other countries.
 * */

public class DataReducer extends Reducer<Text, DataWritable, Text, Text>
{
	public void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException
	{
		int targetColumn = Integer.parseInt(context.getConfiguration().get("targetColumn"));
		List<Integer> dependentColumns = getDependentColumns(context.getConfiguration().get("byColumns"));
		String reducerType = context.getConfiguration().get("reducerType");
		
		HashMap<String, List<Long>> dataMap = generateHashMap(values, targetColumn, dependentColumns);

		switch (reducerType)
		{
			case "sum":
				sumReducer(key, context, dataMap);
				break;
			case "min":
				minReducer(key, context, dataMap);
				break;
			case "max":
				maxReducer(key, context, dataMap);
				break;
			case "avg":
				averageReducer(key, context, dataMap);
				break;
			case "med":
				medianReducer(key, context, dataMap);
				break;
			default:
				stdReducer(key, context, dataMap);
				break;
		}
	}
	
	private List<Integer> getDependentColumns(String str)
	{
		List<Integer> columns = new ArrayList<Integer>();
		
		return columns;
	}
	
	private HashMap<String, List<Long>> generateHashMap(Iterable<DataWritable> values, Integer targetColumn, List<Integer> dependentColumns)
	{
		HashMap<String, List<Long>> dataMap = new HashMap<String, List<Long>>();
		
		for (DataWritable value: values)
		{
			if (!dataMap.containsKey(value.getKey()))
				dataMap.put(value.getKey(), new ArrayList<Long>());

			dataMap.get(value.getKey()).add(value.getValue());
		}
		
		return dataMap;
	}

	private void sumReducer(Text key, Context context, HashMap<String, List<Long>> dataMap) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> curr: dataMap.entrySet())
		{
			long sum = getSum(curr.getValue());
	
			context.write(key, new Text(curr.getKey() + ": SUM -> " + sum));
		}
	}
	
	private void averageReducer(Text key, Context context, HashMap<String, List<Long>> dataMap) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> data: dataMap.entrySet())
		{
			double avg = getAverage(data.getValue());

			context.write(key, new Text(data.getKey() + ": AVG -> " + avg));
		}
	}
	
	private void medianReducer(Text key, Context context, HashMap<String, List<Long>> dataMap) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> data: dataMap.entrySet())
		{
			int midPoint = (data.getValue().size() - 1) / 2;
			long median = data.getValue().get(midPoint);

			context.write(key, new Text(data.getKey() + ": MED -> " + median));
		}
	}
	
	private void minReducer(Text key, Context context, HashMap<String, List<Long>> dataMap) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> data: dataMap.entrySet())
		{
			long min = getMin(data.getValue());

			context.write(key, new Text(data.getKey() + ": MIN -> " + min));
		}
	}
	
	private void maxReducer(Text key, Context context, HashMap<String, List<Long>> dataMap) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> data: dataMap.entrySet())
		{
			long max = getMax(data.getValue());		

			context.write(key, new Text(data.getKey() + ": MAX -> " + max));
		}
	}
	
	private void stdReducer(Text key, Context context, HashMap<String, List<Long>> dataMap) throws IOException, InterruptedException
	{
		for (Map.Entry<String, List<Long>> data: dataMap.entrySet())
		{
			double std = 0;
	
			if (data.getValue().size() > 1)
			{
				double avg = getAverage(data.getValue());
				double var = 0;
				
				for (long val: data.getValue())
					var += Math.pow(val - avg, 2);
				
				var /= (data.getValue().size() - 1);
				std = Math.sqrt(var);
			}

			context.write(key, new Text(data.getKey() + ": STD -> " + std));
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