package com.sumit.Hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends 
         Reducer<Text, LongWritable, Text, LongWritable>{
	@Override
	protected void reduce(Text Key, Iterable<LongWritable> values,Context context)
		throws IOException, InterruptedException {
	
		//sum the list of values
		long sum = 0;
		for (LongWritable value : values) {
			sum = sum + value.get();		
		}
		
		//assign the sum to correspondence word or keys
		context.write(Key, new LongWritable(sum));

	}

}
