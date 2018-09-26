package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import org.apache.log4j.Logger;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<LongWritable,Text, WordPair, LongWritable>{
	
//	private Text outputKey = new Text();
	private HashMap<WordPair,Integer> innerBuffer;
//	private MapWritable occurrenceMap = new MapWritable();
//	private Text word = new Text();
//	private WordPair wordPair = new WordPair();
    private static LongWritable ONE = new LongWritable(1);
	private final static Logger LOGGER = Logger.getLogger(WordCooc.class.getName());
 
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	
		innerBuffer = new HashMap<WordPair, Integer>();
    }
	
	@Override
	public void map(LongWritable inputKey,Text value, Context context) throws IOException, InterruptedException {
		
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
		
		// test
		WordPair a = new WordPair();
    	a.setWord("a first"); a.setNeighbor("a second");
    	WordPair b = new WordPair();
    	b.setWord("b first"); b.setNeighbor("b second");
    	
    	LOGGER.info("test if a = b : "+ a.equals(b));
    	
		// Set the window with a value of 2. This means the co-occurrence for a word include the two words before it and the two words after it.
		int neighbours = context.getConfiguration().getInt("neighbors", 2);
		
		// split by space or multiple space.
        String[] tokens = value.toString().split("\\W+");
        if (tokens.length > 1) {
        	
        	
        	for (int i = 0; i < tokens.length; i++) {
//        		wordPair.setWord(tokens[i]);
        		int start = 0 ;
        		int end = i + neighbours;
        		
        		// adjust the start and end if they are out of boundary
	             if (i - neighbours >= 0) {
	            	 start = i - neighbours;
	             }
	            
	             if (end >= tokens.length - 1) {
	            	 end =  tokens.length - 1;
	             }	          
	             
	             for (int j = start; j <= end ; j++) {
	                  if (j == i) continue;
	                  WordPair newPair = new WordPair();
	                  newPair.setWord(tokens[i]);
	                  newPair.setNeighbor(tokens[j]);
	                  
	                   LOGGER.info("word pair " + newPair.toString());
	                   LOGGER.info("if contained? " + innerBuffer.containsKey(newPair));
	                   
	                   if (innerBuffer.containsKey(newPair)) {
	                	   innerBuffer.put(newPair, innerBuffer.get(newPair) + 1);
	                	   LOGGER.info("contained " + innerBuffer.get(newPair));
	                   } else {
	                	   innerBuffer.put(newPair, 1);
	                	   LOGGER.info("not contained: " + innerBuffer.get(newPair));
	                   }
//	                   context.write(wordPair, ONE);
	             }
	            
          }
      }	
	}

	/****************** local aggregation **********************/
	 @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
		 
         for (WordPair key : innerBuffer.keySet()) {
        	 
        	 LOGGER.info("key "+ key.toString() + "value: "+ innerBuffer.get(key) );
        	 context.write(key, new LongWritable(innerBuffer.get(key)));
        	 
//             context.write(new Text(key), new LongWritable(buffer.get(key)));
         }
     }
}

