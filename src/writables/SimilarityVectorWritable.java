package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SimilarityVectorWritable implements Writable {
	private IntWritable doc2;
	private FloatWritable vector1;
	private FloatWritable vector2;

	public SimilarityVectorWritable() {
		setDoc2(0);
		setVector1(0);
		setVector2(0);
	}
	
	public SimilarityVectorWritable(String doc,String vector1, String vector2) {
		setDoc2(Integer.parseInt(doc));
		setVector1(Float.parseFloat(vector1));
		setVector2(Float.parseFloat(vector2));;
	}
	
	public SimilarityVectorWritable(int doc, float v1, float v2) {
		setDoc2(doc);
		setVector1(v1);
		setVector2(v2);
	}

	public void set(IntWritable doc, FloatWritable v1, FloatWritable v2) {
		this.doc2 = doc;
		this.vector1 = v1;
		this.vector2=v2;
	}
	
	public void setDoc2(int doc) {
		doc2=new IntWritable(doc);
	}
	
	public int getDoc2() {
	    return Integer.parseInt(doc2.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		doc2.readFields(in);
		vector1.readFields(in);
		vector2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		doc2.write(out);
		vector1.write(out);
		vector2.write(out);
	}

	public float getVector1() {
		return Float.parseFloat(vector1.toString());
	}

	public void setVector1(float v1) {
		vector1 = new FloatWritable(v1);
	}

	public float getVector2() {
		return Float.parseFloat(vector2.toString());
	}

	public void setVector2(float v2) {
		vector2 = new FloatWritable(v2);
	}
}


