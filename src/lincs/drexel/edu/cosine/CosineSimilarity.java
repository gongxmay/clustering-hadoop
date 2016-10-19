package lincs.drexel.edu.cosine;

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

public class CosineSimilarity{
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		    String[] parts = value.toString().split("\t");
		    List<String> docList = new ArrayList<String>();
		    List<Double> tfidfList = new ArrayList<Double>();

		    for (int i = 1; i < parts.length; i++){
			    String[] docScore = parts[i].split("@");
			    tfidfList.add(Double.parseDouble(docScore[0]));
			    docList.add(docScore[1]);
		    }

		    //compare documents
		    for (int i=0; i < docList.size(); i++) {
		    	for(int j=i+1; j < docList.size(); j++){
		    		String vector1 = tfidfList.get(i).toString();
		    		String vector2 = tfidfList.get(j).toString();
		    		context.write(new Text(docList.get(i) + " and " + docList.get(j)), new Text(vector1 + "," + vector2));
		    	}
		    }
		}	
	}
	
	public static class WordReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			float crossProduct = 0;
			float docSquaredDistance1 = 0;
			float docSquaredDistance2 = 0;
			for (Text val : values) {
				String[] vectors = val.toString().split(",");
				float vector1 = Float.parseFloat(vectors[0]);
				float vector2 = Float.parseFloat(vectors[1]);
				crossProduct += vector1 * vector2;
				docSquaredDistance1 += vector1 * vector1;
				docSquaredDistance2 += vector2 * vector2;			
			}
			   
			double Denominator=(Math.sqrt(docSquaredDistance1) * Math.sqrt(docSquaredDistance2)); 	 
			double CosineSimilarity = crossProduct / Denominator;
			int doc1 = Integer.parseInt(key.toString().split(" and ")[0]);
			int doc2 = Integer.parseInt(key.toString().split(" and ")[1]);
			context.write(new Text(doc1+" and "+doc2), new Text(String.valueOf(CosineSimilarity)+",0,0"));
			context.write(new Text(doc2+" and "+doc1), new Text(String.valueOf(CosineSimilarity)+",0,0"));
	    }
	}

	public static void calculateCosineSimilarity(String path) throws Exception {		
		Configuration conf = new Configuration();   
	    Job job = new Job(conf, "CosineSimilarity");
	    job.setJarByClass(CosineSimilarity.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);
	    
	    //Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/Postings");
	    Path output=new Path(path+"/pre_Similarity");
	    
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(output))
	      fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}	
}