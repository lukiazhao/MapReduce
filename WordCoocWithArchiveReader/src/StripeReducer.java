import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, WritableHashMap, Text, WritableHashMap> {

	private WritableHashMap reduceMap = new WritableHashMap();

    //@Override
    protected void reduce(Text key, Iterable<WritableHashMap> maps, Context context) throws IOException, InterruptedException {
    	reduceMap.clear();
        // for each HashMap with the same key
        for (WritableHashMap map : maps) {
        	
        	// start adding up the values of each keys in each hashmap. Sum the value up with the same key.
	        Set<Writable> keys = map.keySet();
	        
	        for (Writable k : keys) {
	        	
	        	LongWritable sumForEachKey = (LongWritable) map.get(k);
	        	
	        	if (!reduceMap.containsKey(k)) {
	        		reduceMap.put(k, sumForEachKey);
	        	} else {
	        		LongWritable currentSum = (LongWritable) reduceMap.get(k);
	        		currentSum.set(currentSum.get() + sumForEachKey.get());
	        	}        	
	        }

        }
        context.write(key, reduceMap);
    }
}
