package cosine;

import java.io.IOException;
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

public class TfIdf {

	public static class TF_IDFMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] wordWithDoc = value.toString().split("@");
			String[] docWithCount = wordWithDoc[1].split("\t");
			Text word = new Text(wordWithDoc[0]);
			Text text =new Text(docWithCount[0] + "=" + docWithCount[1]);
			context.write(word, text);
		}	
	}
	
	public static class TF_IDFReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
			int wordCountInCorpus=0;
			Map<String, Double> map=new HashMap<String, Double>();
			
			for (Text val : value) {
				wordCountInCorpus++;
				String[] docWithTf = val.toString().split("=");
				String[]nOverN=docWithTf[1].split("/");
				double WordCountOverWordsPerDoc=Double.parseDouble(nOverN[0].trim())/Double.parseDouble(nOverN[1].trim());
				map.put(docWithTf[0], WordCountOverWordsPerDoc);				
			}
			//Calculate tf*idf
			for (String document : map.keySet()) {
				double tf = map.get(document);
				double preIdf = (double) numOfDocs / (double) wordCountInCorpus;
				double idf= Math.log10(preIdf);
				double tfIdf = idf != 0 ? tf*idf : tf;
				context.write(new Text(key+"@"+document),new Text(String.valueOf(tfIdf)));
			}
		}
	}
	
	public static void calculateTfIdf(String path, int numOfDocs)throws Exception {
		Configuration conf = new Configuration();
		conf.set("numOfDocs",numOfDocs);
	    Job job = new Job(conf, "TfIdf");
	    job.setJarByClass(TfIdf.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(TF_IDFMapper.class);
	    job.setReducerClass(TF_IDFReducer.class);
	    
	    //Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/unique");
	    Path output=new Path(path+"/TF_IDF");
	    
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(output))
	    	fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}
}
