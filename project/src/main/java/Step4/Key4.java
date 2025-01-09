package Step4;

import Step2.Key2;
import Step3.Key3;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Key4 implements WritableComparable<Key4> {
	private final Text firstWord;
	private final Text secondWord;
	private final Text thirdWord;
	DoubleWritable prob;

	public Key4(Key3 key3, double prob) {
		this.firstWord = new Text(key3.getFirstWord().toString());
		this.secondWord = new Text(key3.getSecondWord().toString());
		this.thirdWord = new Text(key3.getThirdWord().toString());
		this.prob = new DoubleWritable(prob);
	}

	//hadoop requires a default constructor
	//Hadoop uses reflection to instantiate Writable objects during runtime.
	public Key4() {
		this.firstWord = new Text("empty");
		this.secondWord = new Text("empty");
		this.thirdWord = new Text("empty");
		this.prob = new DoubleWritable(1);
	}

	public String getFirstWord(){return this.firstWord.toString();}
	public String getSecondWord(){return this.secondWord.toString();}
	public String getThirdWord(){return this.thirdWord.toString();}


	public int size(){
		if(firstWord.toString().equals("*"))
			return 0;
		if(secondWord.toString().equals("*"))
			return 1;
		if(thirdWord.toString().equals("*"))
			return 2;
		return 3;
	}

	@Override
	public String toString() {
		return firstWord.toString() + " " + secondWord.toString() + " " + thirdWord.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Key4) {
			Key4 key = (Key4) obj;
			return firstWord.toString().equals(key.firstWord.toString()) && secondWord.toString().equals(key.secondWord.toString()) && thirdWord.toString().equals(key.thirdWord.toString());
		}
		return false;
	}

	@Override
	/*
	 * given this and other keys, compare them lexicographically.
	 * except for the case when one of the keys is a trigram -
	 * then compare by W1 --> W2 --> -probability
	 */
	public int compareTo(Key4 other){
		if(this.firstWord.toString().equals(other.firstWord.toString()) &&
				this.secondWord.toString().equals(other.secondWord.toString()) &&
				this.thirdWord.toString().equals(other.thirdWord.toString()))
		{return 0;}
		int sizesComb = this.size()*10 + other.size();
		switch(sizesComb){
			case 33:
				if(this.firstWord.toString().equals(other.firstWord.toString())){
					if(this.secondWord.toString().equals(other.secondWord.toString()))
						return other.prob.compareTo(this.prob);
					return this.secondWord.toString().compareTo(other.secondWord.toString());
				} else{
					return this.firstWord.toString().compareTo(other.firstWord.toString());
				}
			default:
				return 0;
		}

	}

	@Override
	//one file
	public int hashCode() {
		return 1;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		firstWord.write(out);
		secondWord.write(out);
		thirdWord.write(out);
		prob.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstWord.readFields(in);
		secondWord.readFields(in);
		thirdWord.readFields(in);
		prob.readFields(in);
	}
}