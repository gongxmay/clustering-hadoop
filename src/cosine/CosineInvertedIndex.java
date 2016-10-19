package cosine;

import java.io.IOException;
        
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CosineInvertedIndex {
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		    String[] wordIdCounter = value.toString().split("\t");
		    String[] wordId = wordIdCounter[0].split("@");
		    String tfIdf = wordIdCounter[1];
		    String word = wordId[0];
		    String document = 	wordId[1];
		    context.write(new Text(word),new Text(tfIdf+"@"+document));
		}	
	}
	
	public static class WordReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			StringBuilder postings = new StringBuilder();
	        //Create the postings list
	        for (Text val : values) {
	        	postings.append(val.toString()+"\t");
	        }
	        context.write(key, new Text(postings.toString()));
	    }
	}
	
	public static void getCosineIndex(String path) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "InvertedIndex");
	    job.setJarByClass(CosineInvertedIndex.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);
	    
	    //Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/TF_IDF");
	    Path output=new Path(path+"/Postings");
	    
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(output))
	      fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}	
}
