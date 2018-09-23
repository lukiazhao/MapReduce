import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements Writable,WritableComparable<WordPair>  {

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
	public int compareTo(WordPair other) {
		int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbour.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbour.compareTo(other.getNeighbor());
	}

}
