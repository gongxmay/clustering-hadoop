package lincs.drexel.edu.ap;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;

import lincs.drexel.edu.ap.ClusterFinder;


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

public class ClusterFinder {
	
    public static class ClusterMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String points = line[0].toString();
			String exemplar = line[1].toString().split(" ")[0];
			String sim = line[1].toString().split(" ")[1];
			context.write(new Text(points), new Text(exemplar + ": " + sim));
		}			
	}

    public static class ClusterReducer extends Reducer<Text, Text, Text, Text> {
	
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		Map<String, Double> simMap = new HashMap<String, Double>();
		    int doc = Integer.parseInt(key.toString());
		    for (Text val : values) {
		        String[] data = val.toString().split(": ");
		        String exemplar = data[0];
		        String sim = data[1];
		        simMap.put(exemplar, Double.parseDouble(sim));
		    }
		    Iterator<String> iter = simMap.keySet().iterator();
		    boolean first = true;
		    double minValue = 0d;
		    String minNo = "0";
		    while (iter.hasNext()) {
		        String exemplar = iter.next().toString();
		        double value = simMap.get(exemplar);
		        if (first) {
		            minNo = exemplar;
		            minValue = value;
		            first = false;
		        }
        	    if (minValue < value) {
        		    minNo = exemplar;
        		    minValue = value;
        	    }
            }
            context.write(key, new Text(minNo));
	    }    
	}			

	public static void getClusters(String path) throws Exception {		
		//N iterations of responsibility and availability
		Configuration conf= new Configuration();
		Job job = new Job(conf);
		job.setJobName("ClusterFinder");
		job.setJarByClass(ClusterFinder.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(ClusterMapper.class);
	    job.setReducerClass(ClusterReducer.class);
	    
	    //Setting the type of input format.
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/Evidence");
	    Path output=new Path(path+"/Cluster");
	    
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(output))
	      fs.delete(output, true);
    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.waitForCompletion(true);
	}
}