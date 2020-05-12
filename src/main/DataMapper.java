package main;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper class for pain pills analysis with big data methods in the USA and some other countries.
 * */

public class DataMapper extends Mapper<LongWritable, Text, Text, DataWritable>
{
	// Mapping the (lineNumber, fullLine) key-pair to (drugName, drugFeatures) key-pair:
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Getting the current full line of the file and transforming it to a word array:
		String lineWords[] = value.toString().split(",");
		
		if (isValid(lineWords)) // Checking if the line does not have empty/null values.
		{
			// Drug name feature: #1 column in the file.
			Text drugName = new Text(lineWords[0]);

			// City feature: #2 column in the file.
			// Sold units feature: #3 column in the file.
			DataWritable data = new DataWritable(lineWords[1], (long)Double.parseDouble(lineWords[2]));

			// Mapping the new key-value pair: 
			context.write(drugName, data);
		}
	}
	
	private boolean isValid(String words[])
	{
		if (words.length != 3)
			return false;

		for (String word: words)
			if (word.equals("null") || word.equals(""))
				return false;
		
		return true;
	}
}