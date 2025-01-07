package Keys;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Key1 implements WritableComparable<Key1> {
    private String firstWord;
    private String secondWord;
    private String thirdWord;

    public Key1(String firstWord, String secondWord, String thirdWord) {
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.thirdWord = thirdWord;
    }

    //hadoop requires a default constructor
    //Hadoop uses reflection to instantiate Writable objects during runtime.
    public Key1() {
        this.firstWord = "empty";
        this.secondWord = "empty";
        this.thirdWord = "empty";
    }

    public int size(){
        if(firstWord.equals("*"))
            return 0;
        if(secondWord.equals("*"))
            return 1;
        if(thirdWord.equals("*"))
            return 2;
        return 3;
    }

    public String getFirstWord(){
        return this.firstWord;
    }

    public String getSecondWord(){
        return this.secondWord;
    }

    public String getThirdWord(){
        return this.thirdWord;
    }

    @Override
    public String toString() {
        return firstWord + " " + secondWord + " " + thirdWord;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key1) {
            Key1 key = (Key1) obj;
            return firstWord.equals(key.firstWord) && secondWord.equals(key.secondWord) && thirdWord.equals(key.thirdWord);
        }
        return false;
    }

    @Override
    /*
     * compareTo method compares the keys lexicographically-like
     * except that "*" is considered to be the smallest word
     */
    public int compareTo(Key1 other){
        if(this.equals(other)){return 0;}
        // compare first word
        if(this.firstWord.equals("*"))
            return -1;
        if(other.firstWord.equals("*"))
            return 1;
        if(this.firstWord.compareTo(other.firstWord) < 0)
            return -1;
        if(this.firstWord.compareTo(other.firstWord) > 0)
            return 1;
        //  compare second word
        if(this.secondWord.equals("*"))
            return -1;
        if(other.secondWord.equals("*"))
            return 1;
        if(this.secondWord.compareTo(other.secondWord) < 0)
            return -1;
        if(this.secondWord.compareTo(other.secondWord) > 0)
            return 1;
        // compare third word
        if(this.thirdWord.equals("*"))
            return -1;
        if(other.thirdWord.equals("*")) 
            return 1;
        if(this.thirdWord.compareTo(other.thirdWord) < 0)
            return -1;
        if(this.thirdWord.compareTo(other.thirdWord) > 0)
            return 1;
        // if all are equal
        return 0;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }


    //TODO: make sure I understand this
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, firstWord);
        Text.writeString(out, secondWord);
        Text.writeString(out, thirdWord);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstWord = Text.readString(in);
        secondWord = Text.readString(in);
        thirdWord = Text.readString(in);
    }
}
