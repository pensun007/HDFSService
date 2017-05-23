package com.laboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
		conf.set("fs.defaultFS","hdfs://localhost:8020");
		
		
		//step -3 Invoke Toolrunner run method
		//It is a generic option parser parsing command line and set 
		//to the configuration.
		
		try {
			
			int i = ToolRunner.run(conf, new HDFSService(), args);
			if(i==0)
			{
				System.out.println("SUCESS");
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
		//Load Configuration
		Configuration conf = super.getConf();

		//Create FileSystem object
		FileSystem hdfs = FileSystem.get(conf);

		//File Write Operation = Creating Metadata + Add Data
		
		//Creating metadata = create empty file
		
		//destination + filename 
		
		final String inputFileName=args[0];
		
		final String actualFileName = getOnlyFileName(inputFileName);
		
		final String hdfsDestinationDir=args[1];
		
		final String emptyFileName = hdfsDestinationDir+"/"+actualFileName;
		
		final Path hdfsEmptyFileName = new Path(emptyFileName);
		
		FSDataOutputStream fsdos=hdfs.create(hdfsEmptyFileName);
		
		//Get the inputStream 
		
		InputStream in = new FileInputStream(inputFileName);
		
		IOUtils.copyBytes(in, fsdos, conf, Boolean.TRUE);
		
		return 0;
	}
	
	private String getOnlyFileName(final String fileNameWithLocation)
	{
		return fileNameWithLocation;
	}
}
