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

import cosine.CosineInvertedIndex;
import cosine.CosineSimilarity;
import cosine.CosineSimilarityComplement;
import cosine.TfIdf;

import sentence.*;
import sun.awt.windows.ThemeReader;

public class AffinityPropagation {

	public static void main(String[] args) throws Exception {							
		if (args.length < 8) {
			System.out.println("Parameters: [path] [fileName] [numOfDocs] [numOfWordInDoc] [choice] [preference] [threshold] [numOfIterations] [NumberofReducers(Optional)]");
			System.exit(1);
		}
						
		String path = args[0];
		String fileName = args[1];
		String numOfDocs = args[2];
		String numOfWordInDoc = args[3];
		String choice = args[4];
		String preference = args[5];
		double threshold = Double.parseDouble(args[6]);
		int numOfIters = Integer.parseInt(args[7]);
		int numOfReducers = args.length == 8 ? 1 : Integer.parseInt(args[8]);	

        long startWordCount = System.nanoTime();
        WordCount.countWord(path, fileName, numOfReducers);
		long startTermFrequecy = System.nanoTime();
		TermFrequency.calculateTF(path, numOfReducers);
		long startInvertedIndex = 0;
		long startSimilarity = 0;
		if (choice.equals("S")) {
			startInvertedIndex = System.nanoTime();
		    TweetInvertedIndex.getInvertedIndex(path, numOfReducers);
		    startSimilarity = System.nanoTime();
		    TwitterSimilarity.calculateSimilarity(path, numOfDocs, numOfWordInDoc, preference, numOfReducers);
		} else {
			TfIdf.calculateTfIdf(path, numOfDocs);
			CosineInvertedIndex.getCosineIndex(path);
			CosineSimilarity.calculateCosineSimilarity(path);
			CosineSimilarityComplement.complementCosineSimilarity(path, numOfDocs, preference);
		}

		//compute elapsed time
		long start = System.nanoTime();
		int numOfIteration = 1;
		long startIter = System.nanoTime();
		numOfIteration = APIteration.iterate(path, numOfDocs, threshold, numOfReducers, numOfIters);
		long startExamplar = System.nanoTime();

		//Calculate responsibility and availability
		ExemplarFinder.findExemplar(path);
		long startCluster = System.nanoTime();
		ClusterFinder.getClusters(path);
		long startResult = System.nanoTime();
		ClusterGenerator.outputClusters(path);
        long endResult = System.nanoTime();
        
		long ElapsedTime=endResult-start;
		float elapsedTimeSec = ElapsedTime;

		long durationWordCount = startTermFrequecy-startWordCount;
		long durationTermFrequency = startInvertedIndex-startTermFrequecy;
		long durationInvertedIndex = startSimilarity-startInvertedIndex;
		long durationSimilarity = startIter-startSimilarity;
		long durationIter = startExamplar-startIter;
		long durationExamplar = startCluster-startExamplar;
		long durationCluster = startResult-startCluster;
		long durationResult = endResult-startResult;
		System.out.println("WordCount Runtime: " + durationWordCount);
		System.out.println("TermFrequency Runtime: " + durationTermFrequency);
		System.out.println("InvertedIndex Runtime: " + durationInvertedIndex);
		System.out.println("Similarity Runtime: " + durationSimilarity);
		System.out.println("Iter Runtime: " + durationIter);
		System.out.println("Examplar Runtime: " + durationExamplar);
		System.out.println("Cluster Runtime: " + durationCluster);
		System.out.println("Result Runtime: " + durationResult);
		System.out.println("Total Runtime: " + ElapsedTime);
	}
}
