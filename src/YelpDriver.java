import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class YelpDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception{
		if(args.length !=2) {
			System.err.println("Usage: YelpDriver usage <input path> <outputpath>");
			System.exit(-1);
		}
	
		Job job = new Job();
		job.setJarByClass(YelpDriver.class);
		job.setJobName("Yelp reviews for user count");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(YelpMapper.class);
		job.setReducerClass(YelpReducer.class);		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		
		System.exit(job.waitForCompletion(true) ? 0:1); 
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		YelpDriver driver = new YelpDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

}
