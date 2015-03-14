import java.io.IOException;
import java.security.AllPermission;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Enridestroy
 * on va compter les elements envoyes par le mapper sur chaque cle.
 * on va renvoyer une paire cle et liste de transactions (uniquement les frequents)
 */
public class AprioriReducer3 extends Reducer<ArrayWritable, ArrayWritable, ArrayWritable, ArrayWritable>{
	private long redCounter;
	
	/**
	 * il faut remplacer ici aussi les Text par ArrayWritable
	 */
	public void reduce(ArrayWritable key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException {
		int frequence = 0;
		//on pourrait utiliser Arrays.asList() puis des remove().
		for (ArrayWritable value : values) {
			MRTransaction[] trx = ((MRTransaction[])value.toArray());
			frequence += trx.length;
		}
		int th = 10;
		//si on en a suffisament,
		if(frequence > th){
			//normalement ca devrait pas marcher mais vu qu'il n'y a qu un item dans values, ca pose pas de pb...
			for (ArrayWritable value : values) {
				context.write(key, value);//est ce que ca fait bien ce que je veux ?
			}
			context.getCounter(Counters.FREQ).increment(1);//il faut juste que des qu'on a un item frequent ca soit + que zero
		}
	}
	
	/**
	 * 
	 */
	@Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job currentJob = cluster.getJob(context.getJobID());
        redCounter = currentJob.getCounters().findCounter(Counters.FREQ).getValue();
    }
}
