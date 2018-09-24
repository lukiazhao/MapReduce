import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.apache.commons.io.IOUtils;


public class PairMapper extends Mapper<Text, ArchiveReader, WordPair, LongWritable>{
	
//	private Text outputKey = new Text();
//	private HashMap<String,Integer> buffer;
//	private MapWritable occurrenceMap = new MapWritable();
//	private Text word = new Text();
	private WordPair wordPair = new WordPair();
    private static LongWritable ONE = new LongWritable(1);
//	private static final Logger LOG = Logger.getLogger(PairMapper.class);

	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        buffer = new HashMap<String, Integer>();
    }
	
	@Override
	public void map(Text key, ArchiveReader value, Context context) throws IOException, InterruptedException {
		
		Iterator<ArchiveRecord> it = value.iterator();
		
		while (it.hasNext()) {
			ArchiveRecord rec = it.next();
			try {
				
				if (rec.getHeader().getMimetype().equals("text/plain")) {
					context.getCounter(MapperCounter.MAPPERCOUNTER.RECORDS_IN).increment(1);
					// Convenience function that reads the full message into a raw byte array
					byte[] rawData = IOUtils.toByteArray(rec, rec.available());
					String content = new String(rawData);
					emitPair(context, content);

				} else {
					context.getCounter(MapperCounter.MAPPERCOUNTER.NON_PLAIN_TEXT).increment(1);
				}
					
			} catch (Exception ex) {
				context.getCounter(MapperCounter.MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		
		}
		
	}
	
	
	protected void emitPair(Context context, String content) throws IOException, InterruptedException {
		// Set the window with a value of 2. This means the co-occurrence for a word include the two words before it and the two words after it.
		int neighbours = context.getConfiguration().getInt("neighbors", 2);
				
		// split by space or multiple space.
        String[] tokens = content.split("\\s+");
		if (tokens.length > 1) {
		        	
			for (int i = 0; i < tokens.length; i++) {
				wordPair.setWord(tokens[i]);
				int start = 0 ;
				int end = i + neighbours;
		        		
		       // adjust the start and end if they are out of boundary
				if (i - neighbours >= 0) {
					start = i - neighbours;
				}
			            
				if (end >= tokens.length ) {
					end =  tokens.length - 1;
				}	          
			             
				for (int j = start; j < end - 1; j++) {
					if (j == i) continue;
					wordPair.setNeighbor(tokens[j]);
					context.write(wordPair, ONE);
				}		             
			}
		}				
	}
}
	
//	 @Override
//     protected void cleanup(Context context) throws IOException, InterruptedException {
//		 
//         for (String key : buffer.keySet()) {
//             context.write(new Text(key), new IntWritable(buffer.get(key)));
//         }
//     }


