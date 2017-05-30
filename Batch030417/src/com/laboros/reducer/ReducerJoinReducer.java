package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoinReducer extends
		Reducer<Text, Text, Text, Text> 
{
	@Override
	protected void reduce(Text key,java.lang.Iterable<Text> values,Context context)
			throws java.io.IOException, InterruptedException 
	{
		//key -- 4000001
		//values -- {CUSTS Kristina	Chung,TXNS	164.93,TXNS	092.88,TXNS	180.35,TXNS	051.18,TXNS	161.34}
		
		//name , total(count++),count(txn_amount)
		
		String name = null;
		double tot= 0;
		int count=0;
		
		for (Text input : values) {
			
			final String inputValue=input.toString();
			
			if(StringUtils.isNotEmpty(inputValue)){
				final String[] tokens =StringUtils.splitPreserveAllTokens(inputValue, "\t");
				
//				tokens[0]==CUST
//				tokens[0]=TXNS
				if(StringUtils.equalsIgnoreCase(tokens[0], "TXNS")){
					count++;
					tot=tot+Double.parseDouble(tokens[1]);
				}else{
					name = tokens[1]+tokens[2];
				}
			}
		}
		if(StringUtils.isNotEmpty(name))
		{
		context.write(new Text(name), new Text(count+"\t"+tot));
		}
	};
  }