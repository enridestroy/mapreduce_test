import java.io.IOException;
import java.security.AllPermission;
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
public class AprioriReducer extends Reducer<Text, MRTransaction, Text, MRTransactionArrayWritable>{
	private long redCounter;
	/**
	 * 
	 */
	public void reduce(Text key, Iterable<MRTransaction> values, Context context) throws IOException, InterruptedException {
		int frequence = 0;
		for (MRTransaction value : values) {
			frequence += 1;//normalement c 1
		}
		int th = 10;
		//si on en a suffisament,
		if(frequence > th){
			MRTransaction[] alltrx = new MRTransaction[frequence];
			//pour chaque transaction valide, on va l'associer a l'item qui faut
			Iterator<MRTransaction> it = values.iterator();
			int i=0;
			while(it.hasNext()){
				//ca devrait faire exploser les data mais tant pis....
				alltrx[i] = it.next();
				i++;
			}
			MRTransactionArrayWritable ooo = new MRTransactionArrayWritable(alltrx);
			//ca devrait etre un ArrayWritable<String> pour la clef
			TextArrayWritable pattern = new TextArrayWritable(new Text[]{ key });//MTN on pourrait ajouter key directement...
			
			context.write(pattern.createText(), ooo);//est ce que ca fait bien ce que je veux ?
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
