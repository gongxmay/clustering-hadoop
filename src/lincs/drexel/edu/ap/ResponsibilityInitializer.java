package lincs.drexel.edu.ap;

import lincs.drexel.edu.writables.DocScores;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ResponsibilityInitializer {
	
	public static class ResponsibilityMap extends Mapper<LongWritable, Text, Text, DocScores> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String[] scores = line[1].split(",");
			String[] documents = line[0].split(" and ");
			
			String doc1 = documents[0];
			String doc2 = documents[1];
			String sim = scores[0];
			String resp = scores[1];
			String avail = scores[2];
	        DocScores outputMap = new DocScores(doc2,sim,resp,avail);
			context.write(new Text(doc1), outputMap);	
		}		
	}
	
	public static class ResponsibilityReduce extends Reducer<Text, DocScores, Text, Text> {
		
		public void reduce(Text key, Iterable<DocScores> values, Context context) throws IOException, InterruptedException {			
			Map<String, String> respMap = new HashMap<String, String>();
		    String doc1 = key.toString();
			Map<Integer, DocScores> docMap = new HashMap<Integer, DocScores>() ;

			for (DocScores val:values) {
				docMap.put(val.getDoc2(), new DocScores(val.getDoc2(), val.getSimilarity(), val.getResponsibility(), val.getAvailibility()));					
			}
				
			// r(i,k) <---- s(i,k) - max {a(i,k')+s(i,k')}				
			// Loop through all doc2s to calculate the r(doc1,doc2)
				
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
				
			for (int doc2 = 1; doc2 <= numOfDocs; doc2++) {
				double newResponsibility;
				if (doc2 == Integer.parseInt(doc1)) {
					double selfSimilarity = docMap.get(doc2).getSimilarity();
					List<Double> maxList = new ArrayList<Double>();
					for (int id = 1; id <= numOfDocs; id++) {
						if (id == doc2)
							continue;			
						maxList.add((double) docMap.get(id).getSimilarity());
					}
					newResponsibility = selfSimilarity-Collections.max(maxList);
				} else {
					double docsSimilarity = docMap.get(doc2).getSimilarity();
					List<Double> maxList = new ArrayList<Double>();
					for (int id = 1; id <= numOfDocs; id++) {
						if(id == doc2)
							continue;
						maxList.add((double) docMap.get(id).getSimilarity() + docMap.get(id).getAvailibility());
					}
					newResponsibility = docsSimilarity - Collections.max(maxList);
				}
				double oldResponsibility = (double) docMap.get(doc2).getResponsibility();
				respMap.put(Integer.toString(doc2), Double.toString(oldResponsibility + newResponsibility));
			}
			
			for (int doc2 = 1; doc2 <= numOfDocs; doc2++) {	
    			context.write(new Text(doc1 + " and " + doc2), new Text(docMap.get(doc2).getSimilarity() + "," + respMap.get(Integer.toString(doc2)) + "," + docMap.get(doc2).getAvailibility())); 	    
			}	
		}			
	}

	public static void initializeResponsibility(String path, String numOfDocs, int numOfReducers) throws Exception{
		Configuration conf= new Configuration();
	    conf.set("numOfDocs", numOfDocs);    
	    
		Job job = new Job(conf);
		job.setJarByClass(ResponsibilityInitializer.class);
		job.setJobName("ResponsibilityInitializer");
		job.setNumReduceTasks(numOfReducers);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		    
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(DocScores.class);
		    
		//Setting the mapper and reducer classes
		job.setMapperClass(ResponsibilityMap.class);
		job.setReducerClass(ResponsibilityReduce.class);
		    
		//Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    Path input=new Path(path+"/Similarity");
	    Path output=new Path(path+"/iteration_output");
	    
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(output))
	        fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    job.waitForCompletion(true);
	}	
}