package sentence;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import writables.docLenWritable;

public class TweetInvertedIndex{

	public static class WordMapper extends Mapper<LongWritable, Text, Text, docLenWritable> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] wordIdCounter = value.toString().split("\t");
			String[] wordId = wordIdCounter[0].split("@");
			String len = wordIdCounter[1].split("/")[1];
			String word = wordId[0];
			String document = wordId[1];
			if(!word.isEmpty()) 
				context.write(new Text(word), new docLenWritable(document, len));
		}
	}

	public static class WordReducer extends Reducer<Text, docLenWritable, Text, Text> {

		public void reduce(Text key, Iterable<docLenWritable> values, Context context) throws IOException, InterruptedException {
			StringBuilder postings = new StringBuilder();
			//Create the postings list
			for (docLenWritable val : values) {
				postings.append(val.getDocument() + "LEN" + val.getLength() + "\t");
			}
			context.write(key, new Text(postings.toString()));
		}
	}

	public static void getInvertedIndex(String path, int numOfReducers) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "tweetInvertedIndex");
		job.setJarByClass(TweetInvertedIndex.class);
		job.setNumReduceTasks(numOfReducers);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(docLenWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Setting the mapper and reducer classes
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);

		//Setting the type of input format.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path input=new Path(path+"/unique");
		Path output=new Path(path+"/Postings");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output))
			fs.delete(output, true);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}	
}