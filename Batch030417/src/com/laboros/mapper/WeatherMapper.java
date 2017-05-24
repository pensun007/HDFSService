package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key,Text value,Context context)
			throws java.io.IOException, InterruptedException 
	{
		//key -- 0
		//value --26565 20140627  2.514 -148.46   70.16 -9999.0
		//-9999.0 -9999.0 -9999.0 -9999.0 -9999.00 U -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0

		//step - 1 : Convert to string
		
		final String iLine = value.toString();
		
		//Step - 2 : Check null
		
		if(StringUtils.isNotEmpty(iLine))
		{
			
			final String date=StringUtils.substring(iLine, 6, 14).trim();
			final String year=StringUtils.substring(date, 0, 4).trim();
			final String max_temp=StringUtils.substring(iLine, 38, 45).trim();
			
			context.write(new IntWritable(Integer.parseInt(year)), new Text(date+"\t"+max_temp));
			
		}
	};
}