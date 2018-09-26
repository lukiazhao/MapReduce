package mapreduce;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class StripeMapper extends Mapper<LongWritable,Text,Text, WritableHashMap> {
	private Text word = new Text();
	private WritableHashMap occurenceMap = new WritableHashMap();
	private final static Logger LOGGER = Logger.getLogger(StripeMapper.class.getName());

	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int neighbors = context.getConfiguration().getInt("neighbors", 2);
		String[] tokens = value.toString().split("\\W+");
		
		if (tokens.length > 1) {
			for (int i = 0; i < tokens.length; i++) {
		          word.set(tokens[i]);
		          occurenceMap.clear();
		          LOGGER.info("word:" + word.toString());
		          int start = 0 ;
		          int end = i + neighbors;
	        		
	        		// adjust the start and end if they are out of boundary
		          if (i - neighbors >= 0) {
		        	  start = i - neighbors;
		          }
		            
		          if (end >= tokens.length - 1 ) {
		        	  end =  tokens.length - 1;
		          }	  
		             
		           for (int j = start; j <= end; j++) {
		                if (j == i) continue;		// skip it
		                
		                Text neighbor = new Text(tokens[j]);
				          LOGGER.info("neighbour:" + neighbor.toString());
				          
		                if(occurenceMap.containsKey(neighbor)){
		                	
		                	LongWritable sum = (LongWritable) occurenceMap.get(neighbor);
		                	sum.set(sum.get() + 1);
		                	occurenceMap.put(neighbor, sum);
		  		          LOGGER.info("sum for :" + neighbor.toString() + " is " + sum);

		                   
		                } else {
		                	
		                	occurenceMap.put(neighbor,new LongWritable(1));
			  		          LOGGER.info("sum for :" + neighbor.toString() + " is " + 1);

		                }
		           }
		           
		           context.write(word, occurenceMap);
		     }
			
		} else {
			
			context.write(new Text("token <= 1"), occurenceMap);
		}

	}
//	
//	 @Override
//     protected void cleanup(Context context) throws IOException, InterruptedException {
//		 
//         for (Writable key : occurenceMap.keySet()) {
//        	 
//        	 LOGGER.info("key : "+ key.toString() + "; value: "+ occurenceMap.get(key) );
////        	 context.write(key, new LongWritable(innerBuffer.get(key)));
//        	 
////             context.write(new Text(key), new LongWritable(buffer.get(key)));
//         }
//     }
}
