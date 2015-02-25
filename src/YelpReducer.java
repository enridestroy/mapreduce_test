import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Enridestroy
 *
 */
public class YelpReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)	throws IOException, InterruptedException {
		//int maxValue = Integer.MIN_VALUE;
		int total = 0;
		for (IntWritable value : values) {
			total += value.get();//normalement, juste en faisant values.size ca marche mais bon...
		}
		context.write(key, new IntWritable(total));
	}
}
