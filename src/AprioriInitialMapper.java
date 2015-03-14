import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Enridestroy
 *
 */
public class AprioriInitialMapper extends Mapper<LongWritable, Text, Text, MRTransaction>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(" ");
		//crer une MRTransaction
		MRTransaction trx = new MRTransaction(items);		
		for(String item : items){
			context.write(new Text(item), trx);
		}
	}

}
