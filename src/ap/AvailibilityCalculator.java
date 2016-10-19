package ap;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import writables.DocScores;

public class AvailibilityCalculator {
	
	public static class AvailibilityMap extends Mapper<LongWritable, Text, Text, DocScores> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String[] scores = line[1].split(",");
			String[] documents = line[0].split(" and ");
			
			String doc1 = documents[0];
			String doc2 = documents[1];
			String sim = scores[0];
			String resp = scores[1];
			String avail = scores[2];
			
	        DocScores outputMap = new DocScores(doc1,sim,resp,avail);
			context.write(new Text(doc2),outputMap);	
		}		
	}
	
	public static class AvailibilityReduce extends Reducer<Text, DocScores, Text, Text> {
		
		public void reduce(Text key, Iterable<DocScores> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));

			//Calculate availability
			Map<String, String> availibility = new HashMap<String, String>();	
			String doc2 = key.toString();
			Map<Integer,DocScores> docMap = new HashMap<Integer,DocScores>();

			for (DocScores val:values) {
				int doc1 = val.getDoc2();
				docMap.put(doc1,new DocScores(Integer.parseInt(doc2), val.getSimilarity(), val.getResponsibility(), val.getAvailibility()));	
			}
			
			//a(i,k)<----- min{0,r(k,k) + SUM(MAX(0,r(i',k))}
			for (int docid1 = 1; docid1 <= numOfDocs; docid1++) {
			    long t1 = System.nanoTime();
				double newAvailibility = 0d;
				if (docid1 == Integer.parseInt(doc2)) {
					double sumResp = 0d;
					for (int docid2 = 1; docid2 <= numOfDocs; docid2++) {
						if(docid2 == docid1)
							continue;
						if(docMap.get(docid2).getResponsibility() > 0)
							sumResp += docMap.get(docid2).getResponsibility();
					}
					newAvailibility = sumResp;
				} else {
					double selfResponsibility = (double) docMap.get(Integer.parseInt(doc2)).getResponsibility();
					double sumResp = 0d;
					for (int docid2 = 1; docid2 <= numOfDocs; docid2++ ) {
						if (docid2 == docid1 || docid2 == Integer.parseInt(doc2))
							continue;
						if (docMap.get(docid2).getResponsibility() > 0)
							sumResp += docMap.get(docid2).getResponsibility();
					}
					newAvailibility = selfResponsibility + sumResp < 0 ? selfResponsibility + sumResp : 0d;
				}
				
				double oldAvailbility = docMap.get(docid1).getAvailibility();
				
				double avail = 0.5*oldAvailbility + 0.5*newAvailibility;
				
				availibility.put(Integer.toString(docid1), Double.toString(avail));
			}
			
			//Output availability
			for(int doc1 = 1; doc1 <= numOfDocs; doc1++){		
				String avail = availibility.get(Integer.toString(doc1));
				context.write(new Text(doc1 + " and " + doc2), new Text(docMap.get(doc1).getSimilarity() + "," + docMap.get(doc1).getResponsibility() + "," + avail)); 
			}			
        }		
	}

	public static void updateAvailibility(String path, String numOfDocs, int Reducers) throws Exception {	
		Configuration conf= new Configuration();
		conf.set("numOfDocs",numOfDocs);    

		Job job = new Job(conf);
		job.setJarByClass(AvailibilityCalculator.class);
		job.setJobName("AvailibitilyCalculation");
		job.setNumReduceTasks(Reducers);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		   
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DocScores.class);
		    
		//Setting the mapper and reducer classes
		job.setMapperClass(AvailibilityMap.class);
		job.setReducerClass(AvailibilityReduce.class);
		    
		//Setting the type of input format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    Path input=new Path(path+"/iteration_output");
		Path output=new Path(path+"/Similarity");
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output))
		  fs.delete(output, true);
		    
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		 
		job.waitForCompletion(true);	
	}	
}
