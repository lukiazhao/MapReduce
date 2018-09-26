package mapreduce;

import java.io.DataInput;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements Writable, WritableComparable<WordPair>  {
	private final static Logger LOGGER = Logger.getLogger(WordPair.class.getName());

	private Text word;
	private Text neighbour;
	
	public WordPair(Text word, Text neighbour) {
	        this.word = word;
	        this.neighbour = neighbour;
	}
	
	public WordPair() {
	        this.word = new Text();
	        this.neighbour = new Text();
	 }
	public void setWord(String wordIn) {
		this.word.set(wordIn);
	}
	
	public void setNeighbor(String neighbour){
        this.neighbour.set(neighbour);
    }
	
	//@Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighbour.readFields(in);
    }

  //@Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighbour.write(out);
    }


    public Text getNeighbor() {
        return neighbour;
    }
    
    public Text getWord() {
        return word;
    }
    
    @Override
    public String toString() {
        return "{word=["+word+"]"+
               " neighbor=["+ neighbour +"]}";
    }

    @Override
    public int hashCode() {
    	int code = 0;
    	int neighCode = 0;
    	if (this.word != null) {
    		code = this.word.hashCode();
    	}
    	if(this.neighbour != null) {
    		neighCode = this.neighbour.hashCode();
    	}
    	int result = 163 * code + neighCode;
		return result;
    	
    }
    
    @Override 
    public boolean equals(Object other) {
//    	if (this == other) return true;
//    	// test if they have same data type first
//        if (other == null || getClass() != other.getClass()) return false;
//
//        WordPair otherPair = (WordPair) other;
//        if ((this.word.equals(otherPair.word) && this.neighbour.equals(otherPair.neighbour)) || 
//        		(this.word.equals(otherPair.neighbour) && this.neighbour.equals(otherPair.word))){
//        	return true;
//        } else {
//        	return false;
//        }        
    
//    	 if (this == other) return true;
         if (other == null || getClass() != other.getClass()) return false;
         WordPair wordPair = (WordPair) other;
         LOGGER.info("equals" + this.toString() + wordPair.toString());
         if (neighbour != null ? !(neighbour.toString()).equals(wordPair.neighbour.toString()) : wordPair.neighbour != null) return false;
         if (word != null ? !(word.toString()).equals(wordPair.word.toString()) : wordPair.word != null) return false;

         return true;

    }
    
    
	@Override
	public int compareTo(WordPair other) {
		int comp = this.word.compareTo(other.getWord());
        if(comp != 0){
            return comp;
        }       
        if(this.neighbour.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbour.compareTo(other.getNeighbor());
	}

}
