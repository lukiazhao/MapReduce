import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
	
	@Override
	public void reduce(WordPair Key,Iterable<LongWritable> values,Context output) throws IOException,InterruptedException {
	           
	     int sum = 0;
	     for(LongWritable value: values) {
	          sum += value.get();
	      }
	
	    output.write(Key,new LongWritable(sum));
	}
	
	 @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
         super.cleanup(context);
     }

}
