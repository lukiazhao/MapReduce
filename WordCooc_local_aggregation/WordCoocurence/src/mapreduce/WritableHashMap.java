package mapreduce;

import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class WritableHashMap extends MapWritable {
	
	public WritableHashMap() {
		
	}
	
	@Override
	   public String toString(){
	      String s = new String("{ ");
	      Set<Writable> keys = this.keySet();
	      
	      for(Writable key : keys){
	    	  
	         LongWritable count = (LongWritable) this.get(key);
	         s =  s + key.toString() + "=" + count.toString() + ",";
	         
	      }
	
	      s = s + " }";
	      return s;
	   }
}
