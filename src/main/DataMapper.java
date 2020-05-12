package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper class for the Hadoop Project.
 * */

public class DataMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	// Mapping the (lineNumber, fullLine) key-pair to (dataKey, dataValue) key-pair:
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Getting the current full line of the file and transforming it to a word array:
		String[] lineWords = value.toString().split(",");

		int targetColumn = Integer.parseInt(context.getConfiguration().get("targetColumn")) - 1;
		List<Integer> dependentColumns = getDependentColumns(context.getConfiguration().get("dependentColumns"));
		int maxColumnNumber = getMaxColumnNumber(targetColumn, dependentColumns);

		if (isValidRow(lineWords, targetColumn, maxColumnNumber))
		{
			String outputKeyStr = dependentColumns.size() > 0 ? String.valueOf(lineWords[dependentColumns.get(0)]) : "";

			for (int column = 1; column < dependentColumns.size(); ++column)
				outputKeyStr += ", " + String.valueOf(lineWords[dependentColumns.get(column)]);

			Text outputKey = new Text(outputKeyStr);
			DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(lineWords[targetColumn]));

			// Mapping the new key-vtargetColumnalue pair: 
			context.write(outputKey, outputValue);
		}
	}

	private List<Integer> getDependentColumns(String str)
	{
		List<Integer> columns = new ArrayList<Integer>();
		String[] numStrs = str.split(",");

		for (String numStr: numStrs)
			columns.add(Integer.parseInt(numStr) - 1);

		return columns;
	}

	private boolean isValidRow(String[] words, int targetColumn, int maxColumnNumber)
	{
		if (words.length < maxColumnNumber)
			return false;

		try
		{
			Double.parseDouble(words[targetColumn]);
		}
		catch (NumberFormatException e)
		{
			return false;
		}

		for (String word: words)
			if (word.equals("null") || word.equals(""))
				return false;

		return true;
	}

	private int getMaxColumnNumber(int targetColumn, List<Integer> dependentColumns)
	{
		int max = targetColumn;
		
		for (int val: dependentColumns)
			if (val > max)
				max = val;
		
		return max;
	}
}