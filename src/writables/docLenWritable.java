package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class docLenWritable implements Writable {

	private IntWritable word;
	private IntWritable count;


	//null arguments
	public docLenWritable(){
		setDocument(0);
		setLength(0);
	}
	
	public docLenWritable(String word,String c) {
		setDocument(Integer.parseInt(word));
		setLength(Integer.parseInt(c));
	}
	
	public docLenWritable(int word, int v1) {
		setDocument(word);
		setLength(v1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		word.write(out);
		count.write(out);
	}

	public int getLength() {
		return count.get();
	}
	
	public void setLength(int count) {
		this.count = new IntWritable(count);
	}
	
	public String getDocument() {
		return word.toString();
	}
	
	public void setDocument(int word) {
		this.word = new IntWritable(word);
	}
}
