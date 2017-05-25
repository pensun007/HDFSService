package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeIdenfityReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	protected void reduce(IntWritable key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {

		String finalDate = null;

		// key -- 2014
		// value {date\tmax_temp,...............}
		// {20140101 -15.6, 20140102 -12.45,......

		float tempMax=Float.MIN_VALUE;
		String fileName=null;
		
		
		for (Text text : values) {

			final String[] tokens = StringUtils.splitPreserveAllTokens(
					text.toString(), "\t");
			
			//tokens[0] = date
			//tokens[1] = max_temp
			
			float iTemp=Float.parseFloat(tokens[1]);
			
			if(iTemp!=-999.99){
				if(tempMax<iTemp){
					tempMax=iTemp;
					finalDate=tokens[0];
					fileName=tokens[2];
				}
			}
		}
		context.write(key, new Text(finalDate+"\t"+tempMax+"\t"+fileName));

	};
}