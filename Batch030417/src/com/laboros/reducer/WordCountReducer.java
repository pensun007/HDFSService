package com.laboros.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	protected void reduce(Text key,java.lang.Iterable<IntWritable> values,Context context)
			throws java.io.IOException, InterruptedException 
	{
		//key -- DEER
		//values -- {1,1,1,1,,1}
		int sum=0;
		for (IntWritable value : values) {
			sum = sum+value.get();
		}
		context.write(key, new IntWritable(sum));
	};
  }