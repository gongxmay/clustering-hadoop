package ap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ap.ClusterGenerator;

import sun.awt.windows.ThemeReader;

public class ClusterGenerator {
	
    public static class ResultMapper extends Mapper<LongWritable, Text, Text, Text> {
		
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		    String[] line = value.toString().split("\t");
            String point = line[0];
            String exemplar = line[1];
	        context.write(new Text(exemplar), new Text(point));
	    }				
    }

	public static class ResultReducer extends Reducer<Text, Text, Text, Text> {
	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String points = ""; 
			for (Text val : values){
                points += val+" ";
			}	
			context.write(new Text(key), new Text(points));
		}
	}	

	public static void outputClusters(String path) throws Exception{		      	
		Configuration conf= new Configuration();
		Job job = new Job(conf);
		
		job.setJobName("ClusterGenerator");
		job.setJarByClass(ClusterGenerator.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(ResultMapper.class);
	    job.setReducerClass(ResultReducer.class);
	    
	    //Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    Path input=new Path(path+"/Cluster");
	    Path output=new Path(path+"/Result");
	    
	    FileSystem fs = FileSystem.get(conf); 
	    if (fs.exists(output))
	    	fs.delete(output, true);   
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);	 
	    job.waitForCompletion(true);
	}
}
