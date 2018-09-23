package au.rmit.bde;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GameIMCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    final private static LongWritable ONE = new LongWritable(1);

    private Text tokenValue = new Text();
    HashMap<String, Integer> array=null;
    @Override
    protected  void setup(Context context) throws  IOException {
        array = new HashMap<String, Integer>();
    }

    /**
     * As we have defined the rules for generating key-value from our input files previously,
     * @see GameJob#run(String[])
     *
     * The input parameters of this method are those key value pairs extracted from the input files( recall pics on lecture slides)
     * based on the rule we previously specified.
     *
     * @param offset The number of characters before the first character of current line.
     * @param text The content of the line.
     * @param context A bridge streams output of your mapper algorithm to the framework that will pass it to Reducer nodes later.
     */
    @Override
    protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {


        if (offset.get() == 0L) return;


        String product_title = text.toString().split("\\t")[5];
        String star_rating = text.toString().split("\\t")[7];
        String verify = text.toString().split("\\t")[11];
        String review_body = text.toString().split("\\t")[13];
        review_body = review_body.toLowerCase();
        review_body = review_body.replaceAll("[\"?.,!<>/#]", "");
        if(verify.equals("Y")) {
            for (String token: review_body.split("\\s+")) {
                if(!array.containsKey(token)) {
                    array.put(token, 1);
                } else {
                    array.put(token, array.get(token)+1);
                }
            }
        }
//        int rating = Integer.parseInt(star_rating);
//        if(rating==5)
//        {
//            tokenValue.set(product_title);
//            context.write(tokenValue, ONE);
//        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : array.entrySet()) {
            tokenValue.set(entry.getKey());
            LongWritable count = new LongWritable(entry.getValue());
            context.write(tokenValue, count);
        }
    }
}