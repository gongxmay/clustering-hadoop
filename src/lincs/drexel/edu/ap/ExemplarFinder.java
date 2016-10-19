package lincs.drexel.edu.ap;

import java.io.IOException;
import java.util.HashMap;

import lincs.drexel.edu.ap.ExemplarFinder;
import lincs.drexel.edu.writables.DocScores;

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

public class ExemplarFinder{
	
    public static class ExampleMapper extends Mapper<LongWritable, Text, Text, Text> {
		
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
	        DocScores outputMap2 = new DocScores(doc1,sim,resp,avail);

	        // If doc1 is equal to doc2, then check if it is an exemplar. If yes, emit the output <doc1> <doc2 sim>;
	        // otherwise, emit the output<doc1> <doc2 sim>;
	        if (doc1.equals(doc2) && Double.parseDouble(resp) + Double.parseDouble(avail) > 0)               
			    context.write(new Text(doc1), new Text(outputMap.getDoc2() + "," + outputMap.getSimilarity()));
		    else if (!doc1.equals(doc2))
		    	context.write(new Text(doc2), new Text(outputMap2.getDoc2() + "," + outputMap2.getSimilarity()));
	    }				
    }

	public static class ExampleReducer extends Reducer<Text, Text, Text, Text> {
	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<Integer,Double> simMap = new HashMap<Integer,Double>();
			int doc1 = Integer.parseInt(key.toString());
			for (Text val : values) {
				String[]data = val.toString().split(",");
				String doc2 = data[0];
				String sim = data[1];
				simMap.put(Integer.parseInt(doc2), Double.parseDouble(sim));
			}

			if (simMap.containsKey(doc1)) {
				for (int docid = 1; docid <= simMap.size(); docid++) {		
					context.write(new Text(Integer.toString(docid)), new Text(Integer.toString(doc1) + " " + Double.toString(simMap.get(docid))));
				}
			}	
		}
	}	

	public static void findExemplar(String path) throws Exception{
		Configuration conf= new Configuration();
		Job job = new Job(conf);
		
		job.setJobName("ExemplarFinder");
		job.setJarByClass(ExemplarFinder.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(ExampleMapper.class);
	    job.setReducerClass(ExampleReducer.class);
	    
	    //Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    Path input=new Path(path+"/Similarity");
	    Path output=new Path(path+"/Evidence");
	    
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(output))
	        fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.waitForCompletion(true);
	}
}
