import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
 * il contient des paires <itemsets k-1 frequents, itemsets de niveau k frequents et les transactions qui les contiennent>
 * donc on doit generer des nouveaux itemsets a partir des cles et des suffixes dans les valeurs
 */
public class AprioriReducer2 extends Reducer<Text, SuffixCandANDTrx, Text, MRTransactionArrayWritable>{	
	
	public void reduce(Text key, Iterable<SuffixCandANDTrx> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> candFrag = new ArrayList<>();
		int l = ((Text[])TextArrayWritable.fromText(key).toArray()).length;
		for(SuffixCandANDTrx value : values){
			candFrag.add(value.toString().substring(l, value.getSuffix().size()));//on ajoute les fragments a mixer
		}
		for(int i=0;i<candFrag.size();i++){
			for(int j=(i+1);j<candFrag.size();j++){
				//il faut remplir cette liste comboTrx.
				//a quel moment on compte les listes qui respectent le motif ?
				//ici, on doit toutes les passer (celles des generateurs)
				MRTransactionArrayWritable comboTrx = new MRTransactionArrayWritable();
				//if(i==j) continue;
				//pour chaque nouvelle combinaison, il faut combiner leurs listes respectives ?
				
				/**
				 * comment ?
				 */
				
				//key de base
				ArrayList<Text> capiquelesyeux = (ArrayList<Text>)(Arrays.asList((Text[])TextArrayWritable.fromText(key).toArray()));
				capiquelesyeux.add(new Text(candFrag.get(i)));
				capiquelesyeux.add(new Text(candFrag.get(j)));
				//context.write(new Text(key+candFrag.get(i)+candFrag.get(j)) , comboTrx);//on cree des nouveaux candidats
				context.write(new TextArrayWritable(capiquelesyeux.toArray(new Text[capiquelesyeux.size()])).createText(), comboTrx);//on cree des nouveaux candidats
			}
		}	
	}
}
