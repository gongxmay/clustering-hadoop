package ap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class APIteration {	
	public static int iterate (String path, String numOfDocs, double threshold, int numOfReducers, int numOfIters) throws Exception {	
		int sameCount = 0;
		boolean accumulatedSameFlag = false;		
		ResponsibilityInitializer.initializeResponsibility(path,numOfDocs,numOfReducers);

		int numOfIteration = 0;
		int maxNumOfIteration = numOfIters;
		double oldSim[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double oldResp[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double oldAvail[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double newSim[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double newResp[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double newAvail[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		for (int doc1 = 0; doc1 < Integer.parseInt(numOfDocs); doc1++) {
			for (int doc2 = 0; doc2 < Integer.parseInt(numOfDocs); doc2++) {
				oldSim[doc1][doc2] = 0d;
				oldResp[doc1][doc2] = 0d;
				oldAvail[doc1][doc2] = 0d;
			}
		}

		//Iterate until converges or after 100 iterations.
		for (int i=0; i < maxNumOfIteration; i++) {
			numOfIteration = i + 1;
			AvailibilityCalculator.updateAvailibility(path, numOfDocs, numOfReducers);	
			Configuration conf = new Configuration();
			ResponsibilityCalculator.updateResponsibility(path, numOfDocs, numOfReducers);
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

			FileSystem fileSystemIn = FileSystem.get(conf);
			FileStatus[] status = fileSystemIn.listStatus(new Path(path+"/Similarity"));
			for (int fileId = 0; fileId < status.length; fileId++) {
				if (!status[fileId].getPath().toString().contains("part"))
					continue;
				FSDataInputStream br = fileSystemIn.open(status[fileId].getPath());
				String strLine = "";
				while ((strLine = br.readLine()) != null) {
					String line[] = strLine.split("\t");
					int row = Integer.parseInt(line[0].split(" and ")[0]);
					int colum = Integer.parseInt(line[0].split(" and ")[1]);
					newSim[row-1][colum-1] = Double.parseDouble(line[1].split(",")[0]);
					newResp[row-1][colum-1] = Double.parseDouble(line[1].split(",")[1]);
					newAvail[row-1][colum-1] = Double.parseDouble(line[1].split(",")[2]);
				}
			}

			boolean sameFlag = true;
			for (int doc1 = 0; doc1 < Integer.parseInt(numOfDocs); doc1++) {
				for (int doc2 = 0; doc2 < Integer.parseInt(numOfDocs); doc2++) {
					if (Math.abs((newAvail[doc1][doc2]+newResp[doc1][doc2])-(oldAvail[doc1][doc2]+oldResp[doc1][doc2])) > threshold) 
						sameFlag=false;
				}
			}
			if (sameFlag && !accumulatedSameFlag) {
				sameCount = 1; 
				accumulatedSameFlag = true;
			} else if (sameFlag && accumulatedSameFlag) {
				sameCount ++;
			} else if (!sameFlag  && accumulatedSameFlag) {
				accumulatedSameFlag = false;
				sameCount = 0;
			}
			//if the evidence doesn't change for 10 iterations, break the loop.
			if (sameCount == 10)
				break;
			for (int doc1 = 0; doc1 < Integer.parseInt(numOfDocs); doc1++){
				for(int doc2 = 0; doc2 < Integer.parseInt(numOfDocs); doc2++){
					oldSim[doc1][doc2] = newSim[doc1][doc2];
					oldResp[doc1][doc2] = newResp[doc1][doc2];
					oldAvail[doc1][doc2] = newAvail[doc1][doc2];
				}
			} 
		}
		return 1;
	}
}