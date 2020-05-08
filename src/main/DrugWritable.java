package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DrugWritable implements Writable
{
	private String name;
	private String city;
	private long units;

	public DrugWritable()
	{
		setValues("", "", 0L);
	}
	
	public void setValues(String name, String city, long units)
	{
		this.name = name;
		this.city = city;
		this.units = units;
	}
	
	public String getName()
	{
		return name;
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
		name = WritableUtils.readString(in);
		city = WritableUtils.readString(in);
		units = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, city);
		out.writeLong(units);
	}

	public String toString()
	{
		return "Name: " + name + ", City: " + city + ", Units: " + units;
	}
}