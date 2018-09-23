import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable,Text,Text, WritableHashMap> {
	private Text word = new Text();
	private WritableHashMap occurenceMap = new WritableHashMap();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int neighbors = context.getConfiguration().getInt("neighbors", 2);
		String[] tokens = value.toString().split("\\s+");
		
		if (tokens.length > 1) {
			for (int i = 0; i < tokens.length; i++) {
		          word.set(tokens[i]);
		          occurenceMap.clear();

		          int start = 0 ;
		          int end = i + neighbors;
	        		
	        		// adjust the start and end if they are out of boundary
		          if (i - neighbors >= 0) {
		        	  start = i - neighbors;
		          }
		            
		          if (end >= tokens.length ) {
		        	  end =  tokens.length - 1;
		          }	  
		             
		           for (int j = start; j < end - 1; j++) {
		                if (j == i) continue;		// skip it
		                
		                Text neighbor = new Text(tokens[j]);
		                
		                if(occurenceMap.containsKey(neighbor)){
		                	
		                	LongWritable sum = (LongWritable) occurenceMap.get(neighbor);
		                	sum.set(sum.get() + 1);
		                   
		                } else {
		                	
		                	occurenceMap.put(neighbor,new LongWritable(1));
		                }
		           }
		           
		           context.write(word, occurenceMap);
		     }
			
		} else {
			
			context.write(new Text("token <= 1"), occurenceMap);
		}

	}
}
