package com.laboros.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSService extends Configured implements Tool {

	public static void main(String[] args) 
	{
		//Step:1 Validating arguments
		
		if(args.length<2)
		{
			//Throw some
			System.out.println("Java Usage: "+HDFSService.class.getName()+" [configuration] /path/to/edge/node/local/input /path/to/hdfs/destination/directory");
			return;
		}
		
		//Step-2 loading configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		//step -3 Invoke Toolrunner run method
		//It is a generic option parser parsing command line and set 
		//to the configuration.
		
		try {
			
			ToolRunner.run(conf, new HDFSService(), args);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
}
