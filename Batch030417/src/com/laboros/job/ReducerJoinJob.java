package com.laboros.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.ReducerJoinReducer;

//This is the driver class name com.laboros.job.WordCountJob

public class ReducerJoinJob extends Configured implements Tool {

	public static void main(String[] args) {
		// Total 3 steps
		// step:1 -- Validating the input arguments
		if (args.length < 3) {
			System.out
					.println("Java Usage "
							+ ReducerJoinJob.class.getName()
							+ " /hdfs/path/to/custs/input /hdfs/path/to/txns/input /hdfs/path/to/output");
			return;
		}
		// step:2 Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);

		try {
			int i = ToolRunner.run(conf, new ReducerJoinJob(), args);
			if (i == 0) {
				System.out.println("SUCCESS");
			} else {
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");

			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// Total 10 steps

		// Step:1 : Get the configuration populated by ToolRunner.run
		Configuration conf = super.getConf();

		// set environment related setting here

		// step-2 create the job instance to get the environment related data
		Job reducerJoinJob = Job.getInstance(conf,
				ReducerJoinJob.class.getName());

		// step-3 : set Jar by class : setting the class path of jar
		// file with the reference of job class at the datanode

		reducerJoinJob.setJarByClass(ReducerJoinJob.class);

		// step-4 setting input

		// step 4-a : Setting customer Input

		final String hdfsCustInput = args[0];
		// Convert to path
		final Path hdfsCustInputPath = new Path(hdfsCustInput);

		MultipleInputs.addInputPath(reducerJoinJob, hdfsCustInputPath,
				TextInputFormat.class, CustMapper.class);
		
		//step 4-b : Setting Transaction input

		final String hdfsTxnsInput = args[1];
		// Convert to path
		final Path hdfsTxnsInputPath = new Path(hdfsTxnsInput);

		MultipleInputs.addInputPath(reducerJoinJob, hdfsTxnsInputPath,
				TextInputFormat.class, TxnMapper.class);
		
		// step-5 : Setting output

		final String hdfsOutput = args[2];
		// Convert to path
		final Path hdfsOutputPath = new Path(hdfsOutput);

		TextOutputFormat.setOutputPath(reducerJoinJob, hdfsOutputPath);
		reducerJoinJob.setOutputFormatClass(TextOutputFormat.class);

		// step-7: Setting Mapper Output Key and Value Class
		reducerJoinJob.setMapOutputKeyClass(Text.class);
		reducerJoinJob.setMapOutputValueClass(Text.class);

		// step - 8 : Setting Reducer
		reducerJoinJob.setReducerClass(ReducerJoinReducer.class);

		// step-9: Setting Reducer Output Key and Value Class
		reducerJoinJob.setOutputKeyClass(Text.class);
		reducerJoinJob.setOutputValueClass(Text.class);

		// Step-10: Trigger Method
		reducerJoinJob.waitForCompletion(Boolean.TRUE);

		return 0;
	}
}
