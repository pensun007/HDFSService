package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key,Text value,Context context)
			throws java.io.IOException, InterruptedException 
	{
		//key -- 0
		//values -- DEER RIVER RIVER
		final String searchString = context.getConfiguration().get("SEARCH_STR");
//		final long iKey = key.get();
		
		final String line=value.toString();
		
		if(StringUtils.isNotEmpty(line))
		{
			final IntWritable ONE=new IntWritable(1);
			final String[] words = StringUtils.splitPreserveAllTokens(line, " ");
			//words[0] = DEER
			//words[1] = RIVER
			//words[2] = RIVER
			Text tWord=new Text();
			for (String word : words) 
			{
				if(StringUtils.equalsIgnoreCase(word, searchString))
				{
				tWord.set(word);
				context.write(tWord, ONE);
				}
			}
		}
			
		{
			
		}
	};
}