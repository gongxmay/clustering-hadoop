package cosine;

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

public class CosineSimilarityComplement {

	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String[] documents = line[0].split(" and ");
			context.write(new Text(documents[0]), new Text(documents[1]+";"+line[1]));	
		}
	}

	public static class WordReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
			int preference = Integer.parseInt(conf.get("preference"));
			HashMap<Integer, String> docMap = new HashMap<Integer,String>();
			
			int doc1 = Integer.parseInt(key.toString());
			for (Text val : values) {
				int doc2 = Integer.parseInt(val.toString().split(";")[0]);
				String sim = val.toString().split(";")[1];
				docMap.put(doc2, sim);
			}
			for (int doc2 = 1; doc2 <= numOfDocs; doc2++){
				if (doc2 == doc1) {
					context.write(new Text(doc2 + " and " + doc2), new Text(Integer.toString(1-preference) + ",0,0"));
				    continue;
				}
				if (docMap.containsKey(doc2)) {
					context.write(new Text(doc1 + " and " + doc2), new Text(docMap.get(doc2)));
				    continue;
				}
				context.write(new Text(doc1 + " and " + doc2), new Text("0,0,0"));
			}
		}
	}_

	public static void complementCosineSimilarity(String path, String numOfDocs, String preference) throws Exception {		
		Configuration conf = new Configuration();
		conf.set("numOfDocs", numOfDocs);
		conf.set("preference", preference);

		Job job = new Job(conf, "CosineSimilarityComplement");
		job.setJarByClass(CosineSimilarityComplement.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Setting the mapper and reducer classes
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);

		//Setting the type of input format.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path input=new Path(path+"/pre_Similarity");
		Path output=new Path(path+"/Similarity");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output))
			fs.delete(output, true);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}	
}