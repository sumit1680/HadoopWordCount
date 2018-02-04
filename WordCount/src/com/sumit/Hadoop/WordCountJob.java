package com.sumit.Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJob implements Tool  {
	
	//intialization the configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;//getting the configuration object
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;// setting the configuration object
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//intialization the job object with configuration
		Job job = new Job(getConf());
		
		//setting the job name
		job.setJobName("WORD COUNT");
		
		//hadoop jar class. detecting the main job
		job.setJarByClass(this.getClass());
		
		//setting the custom mapper class
		job.setMapperClass(WordCountMapper.class);
		
		//setting the custom reducer class
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);//key2
		job.setMapOutputValueClass(LongWritable.class);//value2
		
		job.setOutputKeyClass(Text.class);//key3 or final value
		job.setOutputValueClass(LongWritable.class);//value3 or final
		
		job.setInputFormatClass(TextInputFormat.class); //setting the input format class
		job.setOutputFormatClass(TextOutputFormat.class);//setting the output format class
		
		FileInputFormat.addInputPath(job, new Path(args[0]));//input file or format
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// output file or folder
		
		return	job.waitForCompletion(true) ? 0 : -1;
		
	
	}
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new WordCountJob(), args);
		System.out.println("My Status:" + status);
	}

}
