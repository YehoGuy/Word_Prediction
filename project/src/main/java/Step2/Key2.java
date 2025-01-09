package Step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import Step1.Key1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Key2 implements WritableComparable<Key2> {
	private final Text firstWord;
	private final Text secondWord;
	private final Text thirdWord;
	private final DoubleWritable k3;

	public Key2(Key1 key1) {
		this.firstWord = new Text(key1.getFirstWord().toString());
		this.secondWord = new Text(key1.getSecondWord().toString());
		this.thirdWord = new Text(key1.getThirdWord().toString());
		this.k3 = new DoubleWritable(1);
	}

	public Key2(Key1 key1, double k3) {
		this.firstWord = new Text(key1.getFirstWord().toString());
		this.secondWord = new Text(key1.getSecondWord().toString());
		this.thirdWord = new Text(key1.getThirdWord().toString());
		this.k3 = new DoubleWritable(k3);
	}

	//hadoop requires a default constructor
	//Hadoop uses reflection to instantiate Writable objects during runtime.
	public Key2() {
		this.firstWord = new Text("empty");
		this.secondWord = new Text("empty");
		this.thirdWord = new Text("empty");
		this.k3 = new DoubleWritable(1);
	}

	public String getFirstWord(){return this.firstWord.toString();}
	public String getSecondWord(){return this.secondWord.toString();}
	public String getThirdWord(){return this.thirdWord.toString();}
	public double getK3(){return this.k3.get();}


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
		if (obj instanceof Key2) {
			Key2 key = (Key2) obj;
			return firstWord.toString().equals(key.firstWord.toString()) && secondWord.toString().equals(key.secondWord.toString()) && thirdWord.toString().equals(key.thirdWord.toString());
		}
		return false;
	}

	@Override
	/*
	 * given this and other keys, compare them lexicographically.
	 * except for the case when one of the keys is a trigram -
	 * then compare by W2-->W3-->W1
	 ** we want to achieve:
	 * (W2,*,*)
	 * (W2,...,*)
	 * (W2,W3,*)
	 * (...,W2,W3)
	 * (W1,W2,W3)
	 */
	public int compareTo(Key2 other){
		if(this.firstWord.toString().equals(other.firstWord.toString()) &&
				this.secondWord.toString().equals(other.secondWord.toString()) &&
				this.thirdWord.toString().equals(other.thirdWord.toString()))
		{return 0;}
		int sizesComb = this.size()*10 + other.size();
		switch(sizesComb){
			case 11:
				return this.firstWord.toString().compareTo(other.firstWord.toString());
			case 12:
				if(this.firstWord.toString().equals(other.firstWord.toString()))
					return -1;
				return this.firstWord.toString().compareTo(other.firstWord.toString());
			case 13: // (W2,*,*) < (W1,W2,W3)
				if(this.firstWord.toString().equals(other.secondWord.toString()))
					return -1;
				return this.firstWord.toString().compareTo(other.secondWord.toString());
			case 21:
				if(this.firstWord.toString().equals(other.firstWord.toString()))
					return 1;
				return this.firstWord.toString().compareTo(other.firstWord.toString());
			case 22:
				if(this.firstWord.toString().equals(other.firstWord.toString()))
					return this.secondWord.toString().compareTo(other.secondWord.toString());
				return this.firstWord.toString().compareTo(other.firstWord.toString());
			case 23: //(W2,W3,*) < (W1,W2,W3)
				if(this.firstWord.toString().equals(other.secondWord.toString())){
					if(this.secondWord.toString().equals(other.thirdWord.toString()))
						return -1;
					return this.secondWord.toString().compareTo(other.thirdWord.toString());
				} else{
					return this.firstWord.toString().compareTo(other.secondWord.toString());
				}
			case 31: // (W1,W2,W3) > (W2,*,*)
				if(this.secondWord.toString().equals(other.firstWord.toString()))
					return 1;
				return this.secondWord.toString().compareTo(other.firstWord.toString());
			case 32: // (W1,W2,W3) > (W2,W3,*)
				if(this.secondWord.toString().equals(other.firstWord.toString())){
					if(this.thirdWord.toString().equals(other.secondWord.toString()))
						return 1;
					return this.thirdWord.toString().compareTo(other.secondWord.toString());
				} else{
					return this.secondWord.toString().compareTo(other.firstWord.toString());
				}
			case 33: // compare lexicographically by W2-->W3-->W1
				if(this.secondWord.toString().equals(other.secondWord.toString())){
					if(this.thirdWord.toString().equals(other.thirdWord.toString()))
						return this.firstWord.toString().compareTo(other.firstWord.toString());
					return this.thirdWord.toString().compareTo(other.thirdWord.toString());
				} else{
					return this.secondWord.toString().compareTo(other.secondWord.toString());
				}
			default:
				return 0;
		}

	}

	@Override
	/*
	* we want to achieve:
	* (W2,*,*)
	* (W2,...,*)
	* (W2,W3,*)
	* (...,W2,W3)
	* (W1,W2,W3)
	*/
	public int hashCode() {
		if(size()==1 || size()==2)
			return this.firstWord.toString().hashCode();
		else
			return this.secondWord.toString().hashCode();
	}


	@Override
	public void write(DataOutput out) throws IOException {
		firstWord.write(out);
		secondWord.write(out);
		thirdWord.write(out);
		k3.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstWord.readFields(in);
		secondWord.readFields(in);
		thirdWord.readFields(in);
		k3.readFields(in);
	}
}
