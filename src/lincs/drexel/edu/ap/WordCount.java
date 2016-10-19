package lincs.drexel.edu.ap;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
        
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sun.util.logging.resources.logging;

public class WordCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
        private final static Text word=new Text(); 
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	Set<String> googleStopwords=new HashSet<String>();
    	    googleStopwords.add("I"); googleStopwords.add("a");
 	        googleStopwords.add("an");
 	        googleStopwords.add("are"); googleStopwords.add("as");
        	googleStopwords.add("at"); googleStopwords.add("be");
        	googleStopwords.add("by"); googleStopwords.add("com");
        	googleStopwords.add("do"); googleStopwords.add("if");
        	googleStopwords.add("for"); googleStopwords.add("from");
        	googleStopwords.add("how"); googleStopwords.add("in");
        	googleStopwords.add("is"); googleStopwords.add("it"); 
        	googleStopwords.add("its");googleStopwords.add("it's");
        	googleStopwords.add("la"); googleStopwords.add("of");
        	googleStopwords.add("on"); googleStopwords.add("or");
        	googleStopwords.add("that"); googleStopwords.add("the");
        	googleStopwords.add("that's");
        	googleStopwords.add("this"); googleStopwords.add("to");
        	googleStopwords.add("was"); googleStopwords.add("what");
        	googleStopwords.add("when"); googleStopwords.add("where");
        	googleStopwords.add("who"); googleStopwords.add("wat");
        	googleStopwords.add("with"); googleStopwords.add("and");
        	googleStopwords.add("whose"); googleStopwords.add("www");
        	googleStopwords.add("these");
    	    
        	String[]line=value.toString().toLowerCase().split(" ");
        	String doc_number=line[0];
        	String newLine = value.toString().toLowerCase().trim().replaceAll("\\pP|\\pS|\\pN", " ").replaceAll("\\s{1,}", " ");
        	String[] words = newLine.split(" ");
        	for (int i=0; i<words.length; i++) {
        		if (words[i].length() == 0)
        			continue;
        		word.set(words[i] + "INDOCUMENT" + doc_number);
        		context.write(word, one);
        	}    
        }
    }    

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
        	for (IntWritable val : values) {
            	sum += val.get();
        	}
        	context.write(key, new IntWritable(sum));   
    	}    
	}
        
	public static void countWord(String path, String fileName, int num_Reducer) throws Exception {
	    Configuration conf = new Configuration();
    	Job job = new Job(conf, "WordCount");
    	job.setJarByClass(WordCount.class);
    	job.setNumReduceTasks(num_Reducer);

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    
    	//Setting the mapper and reducer classes
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
    
    	//Setting the type of input format.
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	Path input=new Path(path+"/"+fileName);
    	Path output=new Path(path+"/terms");
    	
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(output))
      		fs.delete(output, true);
    
    	FileInputFormat.addInputPath(job, input);
    	FileOutputFormat.setOutputPath(job, output);
        
    	job.waitForCompletion(true);
	}      
}
