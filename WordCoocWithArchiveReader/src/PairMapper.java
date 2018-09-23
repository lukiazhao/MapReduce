import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
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
	private static final Logger LOG = Logger.getLogger(PairMapper.class);

	
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
					context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
					LOG.info("record???");
					LOG.info(rec.getHeader().getUrl() + " -- " + rec.available());
					// Convenience function that reads the full message into a raw byte array
					byte[] rawData = IOUtils.toByteArray(rec, rec.available());
					String content = new String(rawData);
					
					// Grab each word from the document
					
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
					
					
//					tokenizer = new StringTokenizer(content);
//					if (!tokenizer.hasMoreTokens()) {
//						context.getCounter(MAPPERCOUNTER.EMPTY_PAGE_TEXT).increment(1);
//					} else {
////						while (tokenizer.hasMoreTokens()) {
////							outKey.set(tokenizer.nextToken());
////							context.write(outKey, outVal);
////						}
//					}
				} else {
					context.getCounter(MAPPERCOUNTER.NON_PLAIN_TEXT).increment(1);
				}
					
			} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		
		}
			
			
			
//		// Set the window with a value of 2. This means the co-occurrence for a word include the two words before it and the two words after it.
//		int neighbours = context.getConfiguration().getInt("neighbors", 2);
//				
//		// split by space or multiple space.
//        String[] tokens = value.toString().split("\\s+");
//		if (tokens.length > 1) {
//		        	
//			for (int i = 0; i < tokens.length; i++) {
//				wordPair.setWord(tokens[i]);
//				int start = 0 ;
//				int end = i + neighbours;
//		        		
//		       // adjust the start and end if they are out of boundary
//				if (i - neighbours >= 0) {
//					start = i - neighbours;
//				}
//			            
//				if (end >= tokens.length ) {
//					end =  tokens.length - 1;
//				}	          
//			             
//				for (int j = start; j < end - 1; j++) {
//					if (j == i) continue;
//					wordPair.setNeighbor(tokens[j]);
//					context.write(wordPair, ONE);
//				}		             
//
//			}
		
		// original mapper without any combiner
		/*
	     String line = inputVal.toString();
	     String[] a=line.split("\\W+");

		for (int i=0; i < a.length - 1; i++){
	
			outputKey.set(a[i]+" "+a[i+1]);
			context.write(outputKey,  new IntWritable(1));
	
		}
		*/
		
		
		// in-mapper combiner
		
//		StringTokenizer tokenizer = new StringTokenizer(inputVal.toString());
//		
//        while (tokenizer.hasMoreTokens()) {
//            String word = tokenizer.nextToken();
//            if (buffer.containsKey(word)) {
//                buffer.put(word, buffer.get(word) + 1);
//            } else {
//                buffer.put(word, 1);
//            }
//        }
		
	}
	
	
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EMPTY_PAGE_TEXT,
		EXCEPTIONS,
		NON_PLAIN_TEXT
	}

}
	
//	 @Override
//     protected void cleanup(Context context) throws IOException, InterruptedException {
//		 
//         for (String key : buffer.keySet()) {
//             context.write(new Text(key), new IntWritable(buffer.get(key)));
//         }
//     }


