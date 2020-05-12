package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DrugWritable implements Writable
{
	private String city;
	private long units;
	
	public DrugWritable()
	{
		this("", 0L);
	}

	public DrugWritable(String city, long units)
	{
		this.city = city;
		this.units = units;
	}

	public String getCity()
	{
		return city;
	}
	
	public long getUnits()
	{
		return units;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		city = WritableUtils.readString(in);
		units = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, city);
		out.writeLong(units);
	}
}