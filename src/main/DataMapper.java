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

		// Target column is the value column that will be used for statistical analysis:
		int targetColumn = Integer.parseInt(context.getConfiguration().get("targetColumn")) - 1;

		// Dependent columns will be combined to key values:
		String dependentColStr = context.getConfiguration().get("dependentColumns");
		
		// If the value of the dependent columns is -1, it means there will be only one key:
		List<Integer> dependentColumns = dependentColStr.equals("-1") ? null : getDependentColumns(dependentColStr);

		// We will use maxColumnNumber variable for checking if the current row is valid:
		int maxColumnNumber = dependentColStr.equals("-1") ? 0 : getMaxColumnNumber(targetColumn, dependentColumns);

		if (isValidRow(lineWords, targetColumn, maxColumnNumber))
		{
			String outputKeyStr;

			// If the value of the dependent columns is -1, the key will be "ALL" for every input:
			if (dependentColStr.equals("-1"))
				outputKeyStr = "ALL";
			// Otherwise, we need to add these columns to the key:
			else
			{
				outputKeyStr = String.valueOf(lineWords[dependentColumns.get(0)]);

				for (int column = 1; column < dependentColumns.size(); ++column)
					outputKeyStr += ", " + String.valueOf(lineWords[dependentColumns.get(column)]);
			}

			// Generating new writable key-value pair:
			Text outputKey = new Text(outputKeyStr);
			DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(lineWords[targetColumn]));

			// Mapping the new key-value pair to the reducer: 
			context.write(outputKey, outputValue);
		}
	}

	// Getting the list of dependent columns from a string.
	// Example: "1,4,5" -> [1, 4, 5]
	private List<Integer> getDependentColumns(String str)
	{
		List<Integer> columns = new ArrayList<Integer>();
		String[] numStrs = str.split(",");

		for (String numStr: numStrs)
			columns.add(Integer.parseInt(numStr) - 1);

		return columns;
	}

	// Checking if a row is valid to use or not.
	private boolean isValidRow(String[] words, int targetColumn, int maxColumnNumber)
	{
		// The number of columns in the row must not be less than the number of maximum column number:
		if (words.length < maxColumnNumber)
			return false;

		// The target value must be numeric:
		try
		{
			Double.parseDouble(words[targetColumn]);
		}
		catch (NumberFormatException e)
		{
			return false;
		}

		// Values should not be null or empty:
		for (String word: words)
			if (word.equals("null") || word.equals(""))
				return false;

		return true;
	}

	// Getting the maximum column number from an input:
	private int getMaxColumnNumber(int targetColumn, List<Integer> dependentColumns)
	{
		int max = targetColumn;
		
		for (int val: dependentColumns)
			if (val > max)
				max = val;
		
		return max;
	}
}