package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// key -- 0
		// value -- 4000001,Kristina,Chung,55,Pilot

		final String DATA_SEPERATOR = ",";

		final String iLine = value.toString();

		if (StringUtils.isNotEmpty(iLine)) 
		{
			//split into columns
			final String[] columns = StringUtils.splitPreserveAllTokens(iLine,
					DATA_SEPERATOR);
			
			if(StringUtils.isNotEmpty(columns[0]))
			{
			context.write(new Text(columns[0]), new Text("CUSTS\t"+columns[1]+"\t"+columns[2]));
			}
			
		}

	};
}