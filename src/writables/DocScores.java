package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import ap.AvailibilityCalculator;

public class DocScores implements Writable {
	public IntWritable document_id2;
    public FloatWritable similarity;
    public FloatWritable responsibility;
    public FloatWritable availability;

    public DocScores() {
    	this.document_id2 = new IntWritable(0);
    	this.similarity = new FloatWritable(0);
    	this.responsibility = new FloatWritable(0);
    	this.availability = new FloatWritable(0);
    }
    
    public DocScores(String id2, String s, String r, String a) {
    	this.document_id2 = new IntWritable(Integer.parseInt(id2));
    	this.similarity = new FloatWritable(Float.parseFloat(s));
    	this.responsibility = new FloatWritable(Float.parseFloat(r));
    	this.availability = new FloatWritable(Float.parseFloat(a));
    }

    public DocScores(int d1, int id2, float s, float r, float a) {
    	this.document_id2 = new IntWritable(id2);
    	this.similarity = new FloatWritable(s);
    	this.responsibility = new FloatWritable(r);
    	this.availability = new FloatWritable(a);
    } 
    
    public DocScores(int id2, float s, float r, float a) {
    	this.document_id2 = new IntWritable(id2);
    	this.similarity = new FloatWritable(s);
    	this.responsibility = new FloatWritable(r);
    	this.availability = new FloatWritable(a);
    }
    
    @Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		document_id2.readFields(in);
		similarity.readFields(in);
		responsibility.readFields(in);
		availability.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		document_id2.write(out);
		similarity.write(out);
		responsibility.write(out);
		availability.write(out);
	}
    
    public int getDoc2(){
    	return this.document_id2.get();
    }
    
    public float getSimilarity(){
    	return this.similarity.get();
    }
    
    public float getResponsibility(){
    	return this.responsibility.get();
    }
    
    public float getAvailibility(){
    	return this.availability.get();
    }
}
