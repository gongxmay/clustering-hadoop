package lincs.drexel.edu.sentence;

import lincs.drexel.edu.writables.SimilarityVectorWritable;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterSimilarity {
	
	public static class WordMapper extends Mapper<LongWritable, Text, IntWritable, SimilarityVectorWritable> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			String[] line=value.toString().split("\t");
			Map<String, String> docMap = new HashMap<String, String>();
		    for (int i = 1; i < line.length; i++) {
			    String[] docLen = line[i].split("LEN");
			    docMap.put(docLen[0], docLen[1]);
		    }	    
		    for (String doc1 : docMap.keySet()) {
		    	for(String doc2 : docMap.keySet()) {
		    		context.write(new IntWritable(Integer.parseInt(doc1)), new SimilarityVectorWritable(doc2, docMap.get(doc1), docMap.get(doc2)));
		    	}
		    }	
		}	
    }
	
	public static class WordReducer extends Reducer<IntWritable, SimilarityVectorWritable, Text, Text> {
		
		public void reduce(IntWritable key, Iterable<SimilarityVectorWritable> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
			int numOfWordInDict = Integer.parseInt(conf.get("numOfWordInDict"));
			int preference = Integer.parseInt(conf.get("preference"));
			int doc1 = key.get();
			Map<Integer, String> map = new HashMap<Integer,String>();
		
			double similarity = 0d;
			int numOfWordsInDoc1 = 0;
			for (SimilarityVectorWritable val : values) {
				int doc2 = val.getDoc2();
				numOfWordsInDoc1 = (int) val.getVector1();
				
				if (map.containsKey(doc2)) {
					String tmp = map.get(doc2) + ";" + (int) val.getVector1() + "," + (int) val.getVector2();
					map.put(doc2, tmp);
				} else {
                    map.put(doc2, (int) val.getVector1() + "," + (int) val.getVector2());
				}
			}
			
			for (int docid = 1; docid <= numOfDocs; docid++) {
				if (map.containsKey(docid)) {
					if (doc1 == docid) {
						int NumberOfWordsInDoc = Integer.parseInt(map.get(docid).split(";")[0].split(",")[0]);
						similarity = -Math.log10(numOfWordInDict) * NumberOfWordsInDoc - preference;	
					} else {
						int commonWords = map.get(docid).split(";").length;				
						numOfWordsInDoc1 = Integer.parseInt(map.get(docid).split(";")[0].split(",")[0]);
						int numOfWordsInDoc2 = Integer.parseInt(map.get(docid).split(";")[0].split(",")[1]);
						int uncommonWords=numOfWordsInDoc1 - commonWords;
						similarity = -commonWords * Math.log10(numOfWordsInDoc2) + (-uncommonWords * Math.log10(numOfWordInDict));
					}
				} else {					
					similarity = -Math.log10(numOfWordInDict) * numOfWordsInDoc1;
				}
				context.write(new Text(doc1+" and "+docid),new Text(String.valueOf(similarity)+",0,0"));
			}	
	    }
	}
		
	
	
	public static void calculateSimilarity(String path, String numOfDocs, String numOfWordsInDicts, String preference, int numOfReducers) throws Exception {
		Configuration conf = new Configuration();
	    conf.set("numOfDocs",numOfDocs);    
	    conf.set("numOfWordInDict", numOfWordsInDicts);
	    conf.set("preference", preference);
	    Job job = new Job(conf, "Similarity");
	    
	    job.setJarByClass(TwitterSimilarity.class);
	    job.setNumReduceTasks(numOfReducers);
	    job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SimilarityVectorWritable.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/Postings");
	    Path output=new Path(path+"/Similarity");
	    
	    FileSystem fs = FileSystem.get(conf);    
	    if (fs.exists(output))
	      fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}	
}