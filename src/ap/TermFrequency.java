package ap;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import writables.WordCountWritable;

public class TermFrequency {

	public static class WordMapper extends Mapper<LongWritable, Text, IntWritable,WordCountWritable> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {   
 			String[] wordIdCounter = value.toString().split("\t");
			String[] tweetId = wordIdCounter[0].split("INDOCUMENT");
  			context.write(new IntWritable(Integer.parseInt(tweetId[1])), new WordCountWritable(tweetId[0], wordIdCounter[1]));
		}
	}

	public static class WordReducer extends Reducer<IntWritable, WordCountWritable, Text, Text> {

		public void reduce(IntWritable key, Iterable<WordCountWritable> values, Context context)throws IOException, InterruptedException {
			int Sum = 0;
			Map<String, Integer> tempCounter = new HashMap<String, Integer>();

			//Loop through values and get word occurance and sum of words
			for (WordCountWritable val : values) {
				tempCounter.put(val.getWord(), val.getCount());
				Sum += Integer.valueOf(val.getCount());
			}

			//loop through the map to output reducer job
			for (String word: tempCounter.keySet()) {
				String count = String.valueOf(tempCounter.get(word));
				context.write(new Text(word+"@"+key.toString()), new Text(count+"/"+Sum));
			}
		}
	}

	public static void calculateTF(String path,int numOfReducers) throws Exception {   
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TermFrequency");
		
		job.setJarByClass(TermFrequency.class);
        job.setNumReduceTasks(numOfReducers);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(WordCountWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Setting the mapper and reducer classes
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);

		//Setting the type of input format.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path input=new Path(path+"/terms");
		Path output=new Path(path+"/unique");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output))
			fs.delete(output, true);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}	
}