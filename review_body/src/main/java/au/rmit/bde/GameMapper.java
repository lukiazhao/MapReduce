package au.rmit.bde;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GameMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    final private static LongWritable ONE = new LongWritable(1);
    private static final String TYPE_PHONE = "Phone";
    private Text tokenValue = new Text();


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

        // As the first line of the data file named "will-ockenden-metadata.csv" contains attributes but not actual data
        // the line should be excluded.
        if (offset.get() == 0L) return;

        // For the rest, we are going to compute total phone calls the person made per day.
        String product_title = text.toString().split("\\t")[5];
        String star_rating = text.toString().split("\\t")[7];
        int rating = Integer.parseInt(star_rating);
        if(rating==5)
        {
            tokenValue.set(product_title);
            context.write(tokenValue, ONE);
        }

    }
}