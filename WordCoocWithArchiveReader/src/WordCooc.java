import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import warc.WARCFileInputFormat;

/*
 * For each individual words found in the text and we will track what other words (neighbors) occur within our “window”, 
 * a position relative to the target word. In this test, we use window of "2".
 * For example, consider a part of a sentense “RMIT is a very good university to study at”.
 * With a window value of 2, the co-occurrence for the word “university” would be [very,good,to,study]. 
 * We use two patterns: pair and stripe respectively to investigate their efficiency.
 * */

public class WordCooc extends Configured implements Tool {
	private final static Logger LOGGER = Logger.getLogger(WordCooc.class.getName());

	
	public static void main(String[] args) throws Exception {
		 System.exit(ToolRunner.run(new WordCooc(), args));
	}

	
	@Override
	public int run(String[] args) throws Exception {
		
       Configuration configuration = getConf();
	    configuration.set("mapreduce.job.jar", args[3]);
	    
//		String inputPath = "/tmp/*.warc.wet.gz";
		LOGGER.info("Input path: " + args[1]);

       // create job
       Job job = new Job(configuration, "Cooccurence");
       
       // configure job
       job.setJarByClass(WordCooc.class);
       
       if (args[0].equalsIgnoreCase("pair")) {
       	
	       	LOGGER.info("pairs is selected, start job");
	       	LOGGER.info(" number of args input " + args.length);
	       	job.setJobName("Pairs-cooccurence");
	       	job.setJarByClass(WordCooc.class);
//			job.setNumReduceTasks(1);

	       	
	       	//set input format
	       	job.setInputFormatClass(WARCFileInputFormat.class);
	 	        
	       	//set output format
	       	job.setOutputFormatClass(TextOutputFormat.class);


 	       //set Output key class
 	        job.setOutputKeyClass(WordPair.class);
 	      
 	        //set Output value class
 	        job.setOutputValueClass(LongWritable.class);    
 	        
 	        
	       	//set Mapper, Reducer and Combiner class
	       	job.setMapperClass(PairMapper.class);
	       	job.setReducerClass(PairReducer.class);
	       	job.setCombinerClass(PairReducer.class);
	       
       	
	       	//set file input and output path!
 	        FileInputFormat.addInputPath(job, new Path(args[1]));
 	        FileOutputFormat.setOutputPath(job, new Path(args[2]));
       
       
       } else if (args[0].equalsIgnoreCase("stripe")) {
	       	LOGGER.info("Stripe is selected, start job");
	       	LOGGER.info(" number of args input " + args.length);
	       	job.setJobName("Stripes-cooccurence");
//			job.setNumReduceTasks(1);
	       	
		
	       	//set output key value format
	       	job.setJarByClass(WordCooc.class);
	       	
	       	//set input format
	       	job.setInputFormatClass(WARCFileInputFormat.class);
	 	        
	       	//set output format
	       	job.setOutputFormatClass(TextOutputFormat.class);


 	       //set Output key class
 	        job.setOutputKeyClass(Text.class);
 	      
 	        //set Output value class
			job.setOutputValueClass(WritableHashMap.class);
 	        
 	        
	       	//set Mapper, Reducer and Combiner class
	       	job.setMapperClass(StripeMapper.class);
	       	job.setReducerClass(StripeReducer.class);
	       	job.setCombinerClass(StripeReducer.class);
	       	   	
	       	//set file input and output path
 	        FileInputFormat.addInputPath(job, new Path(args[1]));
 	        FileOutputFormat.setOutputPath(job, new Path(args[2]));
       	
       }
       
	    return job.waitForCompletion(true) ? 0 : -1;
	}
}

