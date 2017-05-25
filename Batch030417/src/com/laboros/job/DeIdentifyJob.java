package com.laboros.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.DeIdentifyMapper;

//This is the driver class name com.laboros.job.WordCountJob

public class DeIdentifyJob extends Configured implements Tool {

	public static void main(String[] args) 
	{
		//Total 3 steps
		//step:1 -- Validating the input arguments
		if(args.length<2)
		{
			System.out.println("Java Usage "+DeIdentifyJob.class.getName()+
					" /hdfs/path/to/input/directory /hdfs/path/to/output");
			return;
		}
		//step:2 Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i=ToolRunner.run(conf, new DeIdentifyJob(), args);
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
		Job deIdentifyJob = Job.getInstance(conf, DeIdentifyJob.class.getName());
		
		//step-3 : set Jar by class : setting the class path of jar 
		//file with the reference of job class at the datanode
		
		deIdentifyJob.setJarByClass(DeIdentifyJob.class);
		
		//step-4 setting input
		
		final String hdfsInput =args[0];
		//Convert to path
		final Path hdfsInputPath = new Path(hdfsInput);
		
		TextInputFormat.addInputPath(deIdentifyJob, hdfsInputPath);
		deIdentifyJob.setInputFormatClass(TextInputFormat.class);
		
		//step-5 : Setting output
		
		final String hdfsOutput=args[1];
		//Convert to path
		final Path hdfsOutputPath=new Path(hdfsOutput);
		
		TextOutputFormat.setOutputPath(deIdentifyJob, hdfsOutputPath);
		deIdentifyJob.setOutputFormatClass(TextOutputFormat.class);
		//step-6 : Setting Mapper
//		deIdentifyJob.setMapperClass(DeIdentifyMapper.class);
		
		//step-7: Setting Mapper Output Key and Value Class
//		deIdentifyJob.setMapOutputKeyClass(Text.class);
//		deIdentifyJob.setMapOutputValueClass(NullWritable.class);
		
		deIdentifyJob.setNumReduceTasks(0);
		//step - 8 : Setting Reducer
//		deIdentifyJob.setReducerClass(DeIdenfityReducer.class);
		
//		step-9: Setting Reducer Output Key aSnd Value Class
//		deIdentifyJob.setOutputKeyClass(Text.class);
//		deIdentifyJob.setOutputValueClass(IntWritable.class);
//		
		//Step-10: Trigger Method
		deIdentifyJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
}
