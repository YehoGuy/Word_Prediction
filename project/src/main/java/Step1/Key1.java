package Step1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import Helpers.Consts;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Key1 implements WritableComparable<Key1> {
    private final Text firstWord;
    private final Text secondWord;
    private final Text thirdWord;

    public Key1(String firstWord, String secondWord, String thirdWord) {
        this.firstWord = new Text(firstWord);
        this.secondWord = new Text(secondWord);
        this.thirdWord = new Text(thirdWord);
    }

    //hadoop requires a default constructor
    //Hadoop uses reflection to instantiate Writable objects during runtime.
    public Key1() {
        this.firstWord = new Text("empty");
        this.secondWord = new Text("empty");
        this.thirdWord = new Text("empty");
    }

    public int size(){
        if(firstWord.toString().equals("*"))
            return 0;
        if(secondWord.toString().equals("*"))
            return 1;
        if(thirdWord.toString().equals("*"))
            return 2;
        return 3;
    }

    public Text getFirstWord(){
        return this.firstWord;
    }

    public Text getSecondWord(){
        return this.secondWord;
    }

    public Text getThirdWord(){
        return this.thirdWord;
    }

    @Override
    public String toString() {
        return firstWord.toString() + " " + secondWord.toString() + " " + thirdWord.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key1) {
            Key1 key = (Key1) obj;
            return firstWord.toString().equals(key.firstWord.toString()) && secondWord.toString().equals(key.secondWord.toString()) && thirdWord.toString().equals(key.thirdWord.toString());
        }
        return false;
    }

    @Override
    /*
     * compareTo method compares the keys lexicographically-like
     * except that "*" is considered to be the smallest word
     */
    public int compareTo(Key1 other){
        if(this.firstWord.toString().equals(other.firstWord.toString()) &&
           this.secondWord.toString().equals(other.secondWord.toString()) &&
           this.thirdWord.toString().equals(other.thirdWord.toString()))
            {return 0;}
        // compare first word
        if(this.firstWord.toString().equals(Consts.STAR))
            return -1;
        if(other.firstWord.toString().equals(Consts.STAR))
            return 1;
        if(this.firstWord.toString().compareTo(other.firstWord.toString()) < 0)
            return -1;
        if(this.firstWord.toString().compareTo(other.firstWord.toString()) > 0)
            return 1;
        //  compare second word
        if(this.secondWord.toString().equals(Consts.STAR))
            return -1;
        if(other.secondWord.toString().equals(Consts.STAR))
            return 1;
        if(this.secondWord.toString().compareTo(other.secondWord.toString()) < 0)
            return -1;
        if(this.secondWord.toString().compareTo(other.secondWord.toString()) > 0)
            return 1;
        // compare third word
        if(this.thirdWord.toString().equals(Consts.STAR))
            return -1;
        if(other.thirdWord.toString().equals(Consts.STAR))
            return 1;
        if(this.thirdWord.toString().compareTo(other.thirdWord.toString()) < 0)
            return -1;
        if(this.thirdWord.toString().compareTo(other.thirdWord.toString()) > 0)
            return 1;
        // if all are equal
        return 0;
    }

    //so that all keys with the same first word will be sent to the same reducer
    @Override
    public int hashCode() {
        return this.firstWord.toString().hashCode();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        firstWord.write(out);
        secondWord.write(out);
        thirdWord.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstWord.readFields(in);
        secondWord.readFields(in);
        thirdWord.readFields(in);
    }
}
