package com.laboros.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.reducer.WordCountReducer;

//This is the driver class name com.laboros.job.WordCountJob

public class WordCountJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		//Total 3 steps
		//step:1 -- Validating the input arguments
		if(args.length<2)
		{
			System.out.println("Java Usage "+WordCountJob.class.getName()+
					" /hdfs/path/to/input /hdfs/path/to/output");
			return;
		}
		//step:2 Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i=ToolRunner.run(conf, new WordCountJob(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			
			e.printStackTrace();
		}
		
	}
	@Override
	public int run(String[] args) throws Exception 
	{
		//Total 10 steps
		
		//Step:1 : Get the configuration populated by ToolRunner.run
		Configuration conf=super.getConf();
		
		//set environment related setting here
		
		//step-2 create the job instance to get the environment related data
		Job wordCountJob = Job.getInstance(conf, WordCountJob.class.getName());
		
		//step-3 : set Jar by class : setting the class path of jar 
		//file with the reference of job class at the datanode
		
		wordCountJob.setJarByClass(WordCountJob.class);
		
		//step-4 setting input
		
		final String hdfsInput =args[0];
		//Convert to path
		final Path hdfsInputPath = new Path(hdfsInput);
		
		TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		//step-5 : Setting output
		
		final String hdfsOutput=args[1];
		//Convert to path
		final Path hdfsOutputPath=new Path(hdfsOutput);
		
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOutputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		//step-6 : Setting Mapper
		wordCountJob.setMapperClass(WordCountMapper.class);
		
		//step-7: Setting Mapper Output Key and Value Class
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		//step - 8 : Setting Reducer
		wordCountJob.setReducerClass(WordCountReducer.class);
		
//		step-9: Setting Reducer Output Key and Value Class
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		
		//Step-10: Trigger Method
		wordCountJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
}
