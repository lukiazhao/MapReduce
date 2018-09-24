import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;


public class StripeMapper extends Mapper<Text, ArchiveReader, Text, WritableHashMap> {
	private Text word = new Text();
	private WritableHashMap occurenceMap = new WritableHashMap();
	
	@Override
	protected void map(Text key, ArchiveReader value, Context context) throws IOException, InterruptedException {
		
		Iterator<ArchiveRecord> it = value.iterator();
		
		while (it.hasNext()) {
			ArchiveRecord rec = it.next();
			try {
				if (rec.getHeader().getMimetype().equals("text/plain")) {
					context.getCounter(MapperCounter.MAPPERCOUNTER.RECORDS_IN).increment(1);
					// Convenience function that reads the full message into a raw byte array
					byte[] rawData = IOUtils.toByteArray(rec, rec.available());
					String content = new String(rawData);
					emitStripe(context, content);
					
				} else {
					context.getCounter(MapperCounter.MAPPERCOUNTER.NON_PLAIN_TEXT).increment(1);
				}	
			} catch (Exception ex) {
				context.getCounter(MapperCounter.MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		}
		
	}
	
	protected void emitStripe(Context context, String content) throws IOException, InterruptedException {
		
		int neighbors = context.getConfiguration().getInt("neighbors", 2);
		String[] tokens = content.split("\\s+");
		
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
		                if (j == i) continue;		// skip the word itself
		                
		                Text neighbor = new Text(tokens[j]);
		                
		                // if the neighbour(key) already exist in the map, add one to its value.
		                // if not, add new key(neighbour)-value(sum) pair to the map.
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
