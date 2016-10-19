package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class ARWritable implements Writable {
	private IntWritable doc2;
	private DoubleWritable a;
	private DoubleWritable r;
	private DoubleWritable similarity;
	
	public ARWritable() {
		setDoc2(0);
		setA(0);
		setR(0);
		setSimilarity(0);
	}
	
	public ARWritable(String doc,String a, String r, String sim) {
		setDoc2(Integer.parseInt(doc));
		setA(Double.parseDouble(a));
		setR(Double.parseDouble(r));
		setSimilarity(Double.parseDouble(sim));
	}
	
	public ARWritable(int doc, double a, double r, double sim) {
		setDoc2(doc);
		setA(a);
		setR(r);
		setSimilarity(sim);
	}

	public void set(IntWritable doc, DoubleWritable a, DoubleWritable r, DoubleWritable sim) {
		this.doc2 = doc;
		this.a = a;
		this.r = r;
		this.similarity = sim;
	}
	
	public void setDoc2(int doc) {
		doc2 = new IntWritable(doc);
	}
	
	public int getDoc2() {
	    return Integer.parseInt(doc2.toString()); 
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		doc2.readFields(in);
		a.readFields(in);
		r.readFields(in);
		similarity.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		doc2.write(out);
		a.write(out);
		r.write(out);
		similarity.write(out);
	}

	public double getA() {
		return Double.parseDouble(a.toString());
	}

	public void setA(double a) {
		this.a = new DoubleWritable(a);
	}

	public double getSimilarity() {
		return Double.parseDouble(similarity.toString());
	}

	public void setSimilarity(double similarity) {
		this.similarity = new DoubleWritable(similarity);
	}

	public double getR() {
		return Double.parseDouble(r.toString());
	}

	public void setR(double r) {
		this.r = new DoubleWritable(r);
	}
}


