package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DataWritable implements Writable
{
	private String key;
	private long value;
	
	public DataWritable()
	{
		this("", 0L);
	}

	public DataWritable(String key, long value)
	{
		this.key = key;
		this.value = value;
	}

	public String getKey()
	{
		return key;
	}
	
	public long getValue()
	{
		return value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = WritableUtils.readString(in);
		value = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, key);
		out.writeLong(value);
	}
}