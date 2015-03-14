import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Enridestroy
 * a la passe 0 on a la liste des transactions en input.
 * on doit les lire, identifier les elements frequents de taille un.
 * on doit ecrire les itemsets frequents avec les transactions qui les contiennent.
 * 
 * apres en passe suivante, on doit generer de nouveaux candidats.
 * pour ca on doit prendre les elements deux par deux qui se suivent ?
 * 
 * faire le reduce peut combiner les valeurs ? ABC et ABD le reduce il a A, B, C puis A, B, C et D 
 * ensuite il ecrit un fichier avec le nouveau candidat et les transactions qui vont bien ?
 * 
 * sur chaque ligne on prend uniquement tout -1. ABC et ABD. on pour cle AB. donc dans le reducer y'aura ABC et ABD et ABE etc...
 * donc le reducer il va ecrire ABCD, ABCE et ABDE.
 * 
 * l'etape suivante c'est de la fouille. donc en entree on veut les transactions qui sont encore utiles et les motifs ?
 * motifs associes avec les transactions.
 * on fouille. et on ecrit quels sont les motifs frequents avec leurs transactions ?
 * 
 * 
 * en sortie du reducer, ya une liste:
 * AB ABCDFE
 * AB KKDOFABE
 * AC ACD
 * DE ADFODL
 * etc...
 * 
 * mapper associe des itemset avec des transaction.
 * 
 * ensuite le reduce il ecrit uniquement les itemset frequent avec les transaction.
 * 
 * ensuite, on lit ce fichier et on mappe les cles-1 avec leur reste. donc on perd les transactions.
 * le reducer va generer les nouveaux itemsets associes a quoi ? rien ? dans l'ideal les transactions.
 * il faut lire les transactions associees a chaque itemset -1 puis les ajouter aux nouveaux itemsets.
 * cad on genere les nouveaux itemsets. pour chaque couple d'itemset, on prend leur liste de transactions et quoi ?
 * 
 * faire un fichier no transact, transact. comme ca on associe pas un itemset a des trx, mais a des trxid.
 * mais il faut quand meme les retrouver plus tard. comment ? HBase ?
 * 
 * on charge les nouveaux itemsets  et quoi ...
 * 
 * TODO :
 * ajouter les nouveaux jobs dans le jobcontroller
 * indiquer les noms des fichiers  a charger...
 */
public class AprioriMRDriver extends Configured implements Tool{
	private String inputPath = "";
	private String outputPath = "";
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Class<? extends Mapper>[] mappers = new Class[]{
		AprioriMapper.class, //retrouve les motifs dans une liste de transactions ?
		AprioriMapper2.class, //trouve les candidats a combiner
		AprioriInitialMapper.class //trouve les motifs de taille 1 dans une liste de trx => decoupe les transactions en items
	};
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Class<? extends Reducer>[] reducers = new Class[]{
		AprioriReducer3.class, //compte les frequences dans les autres niveaux
		AprioriReducer2.class, //genere les nouveaux candidats
		AprioriReducer.class, //compte les frequences au niveau zero
	};
	private String[] jobNames = new String[]{
		"Phase de fouille k=",
		"Generation de candidats k=", 
		"Phase initiale k="
	};
	/**
	 * c'est les trois les memes, donc on devrait en utiliser qu'un seul mais on sait jamais...
	 */
	private Class[][] jobsIOKeys = new Class[][]{
			{ArrayWritable.class, ArrayWritable.class},
			{ArrayWritable.class, ArrayWritable.class},
			{ArrayWritable.class, ArrayWritable.class} //cles io pour initialisation
	};
	private int level = 0;
	/**
	 * 
	 * @param level
	 * @param jobs
	 * @throws IOException 
	 */
	private void addNewLevel(int[] level, List<ControlledJob> jobs) throws IOException{
		this.addCandidateGeneration(level, jobs);
		this.addMiningPhase(level, jobs);
	}
	
	/**
	 * 
	 * @param level
	 * @param jobs
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void addCandidateGeneration(int[] level, List<ControlledJob> jobs) throws IllegalArgumentException, IOException{
		JobConf jc = new JobConf("aaaa"+level[0]);
	    ControlledJob j = new ControlledJob(jc);
	    Job jj = j.getJob();
	    jj.setJarByClass(AprioriMRDriver.class);
	    jj.setJobName("AAA"+level[0]);
		jj.setJobName(this.jobNames[1]+level[0]);
	    j.addDependingJob(jobs.get(jobs.size() - 1));
	    /**
	     * il faut gerer les id des mapper/reducer
	     */
	    jj.setMapperClass(this.mappers[1]);
		jj.setReducerClass(this.reducers[1]);
		//jj.getCounters().addGroup("lol", "lolo");//ajoute un compteur
		jj.setOutputKeyClass(this.jobsIOKeys[1][0]);//ca va dependre de ce qui est code...
		jj.setOutputValueClass(this.jobsIOKeys[1][1]);//ca va dependre de ce qui est code
		
		FileInputFormat.addInputPath(jj, new Path(this.inputPath+"/k_"+(level[0]-1)));//donc le dossier k_0 contient les fichiers avec les items
		//au depart ya rien donc il va lire le fichier automatiquement.
		FileOutputFormat.setOutputPath(jj, new Path(this.outputPath+"/k_"+level[0]));//il va ecrire dans k_1
		//ensuite le job k_2 va lire dans k_1 donc ya qu'un seul fichier.
		jobs.add(j);
	}
	
	/**
	 * 
	 * @param level
	 * @param jobs
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void addMiningPhase(int[] level, List<ControlledJob> jobs) throws IllegalArgumentException, IOException{
		JobConf jc = new JobConf("aaaa"+level[0]);
	    ControlledJob j = new ControlledJob(jc);
	    Job jj = j.getJob();
	    jj.setJarByClass(AprioriMRDriver.class);
	    jj.setJobName("AAA"+level[0]);
		jj.setJobName(this.jobNames[0]+level[0]);
	    j.addDependingJob(jobs.get(jobs.size() - 1));
	    /**
	     * il faut gerer les id des mapper/reducer
	     */
	    jj.setMapperClass(this.mappers[0]);
		jj.setReducerClass(this.reducers[0]);
		//jj.getCounters().addGroup("lol", "lolo");//ajoute un compteur
		jj.setOutputKeyClass(this.jobsIOKeys[0][0]);//ca va dependre de ce qui est code...
		jj.setOutputValueClass(this.jobsIOKeys[0][1]);//ca va dependre de ce qui est code
		
		FileInputFormat.addInputPath(jj, new Path(this.inputPath+"/k_"+(level[0]-1)));//donc le dossier k_0 contient les fichiers avec les items
		//au depart ya rien donc il va lire le fichier automatiquement.
		FileOutputFormat.setOutputPath(jj, new Path(this.outputPath+"/k_"+level[0]));//il va ecrire dans k_1
		//ensuite le job k_2 va lire dans k_1 donc ya qu'un seul fichier.
		jobs.add(j);
	}
	
	/**
	 * 
	 * @param level
	 * @param jobs
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void addFirstPhase(int[] level, List<ControlledJob> jobs) throws IllegalArgumentException, IOException{
		JobConf jc = new JobConf("aaaa"+level[0]);
	    ControlledJob j = new ControlledJob(jc);
	    Job jj = j.getJob();
	    jj.setJarByClass(AprioriMRDriver.class);
	    jj.setJobName("AAA"+level[0]);
		jj.setJobName(this.jobNames[2]+level[0]);
	    j.addDependingJob(jobs.get(jobs.size() - 1));
	    /**
	     * il faut gerer les id des mapper/reducer
	     */
	    jj.setMapperClass(this.mappers[2]);
		jj.setReducerClass(this.reducers[2]);
		//jj.getCounters().addGroup("lol", "lolo");//ajoute un compteur
		jj.setOutputKeyClass(this.jobsIOKeys[2][0]);//ca va dependre de ce qui est code...
		jj.setOutputValueClass(this.jobsIOKeys[2][1]);//ca va dependre de ce qui est code
		
		FileInputFormat.addInputPath(jj, new Path(this.inputPath+"/k_"+(level[0]-1)));//donc le dossier k_0 contient les fichiers avec les items
		//au depart ya rien donc il va lire le fichier automatiquement.
		FileOutputFormat.setOutputPath(jj, new Path(this.outputPath+"/k_"+level[0]));//il va ecrire dans k_1
		//ensuite le job k_2 va lire dans k_1 donc ya qu'un seul fichier.
		jobs.add(j);
	}
	
	/**
	 * 
	 */
	public int run(String[] args) throws Exception{
		if(args.length !=2) {
			System.err.println("Usage: AprioriDriver usage <input path> <outputpath>");
			System.exit(-1);
		}
		
		this.inputPath = args[0];
		this.outputPath = args[1];
		
		//lit un fichier avec les taches minimales ?
		int[] level = new int[]{0};
		List<ControlledJob> jobs = new ArrayList<>();
		this.addFirstPhase(level, jobs);
		this.addMiningPhase(level, jobs);

		/**
		 * si on fait un map => recuperer les patterns
		 * puis reduce qui les compte, 
		 * 
		 * ensuite on a fini donc on doit generer un nouveau job.
		 */
		JobControl jobctrl = new JobControl("controleur de jobs");
	    for(ControlledJob c : jobs)
	    	jobctrl.addJob(c);
	    jobctrl.run();

	    if(this.waitForEnd(level, jobs, jobctrl)){
	    	//waitForEnd(level, jobs, jobctrl);
	    	jobctrl.run();
	    }
	    
		//System.exit(jobs.get(jobs.size() - 1).getJob().waitForCompletion(true) ? 0:1); 
		//boolean success = jobs.get(jobs.size() - 1).getJob().waitForCompletion(true);
		//return success ? 0 : 1;
	    return 0;
	}
	
	/**
	 * 
	 * @param level
	 * @param jobs
	 * @param jobctrl
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private boolean waitForEnd(int[] level, List<ControlledJob> jobs, JobControl jobctrl) throws ClassNotFoundException, IOException, InterruptedException{
		boolean success = jobs.get(jobs.size() - 1).getJob().waitForCompletion(true) ? false : true;
	    return success && this.whatDoWeDoNow(level, jobs, jobctrl);
	}
	
	/**
	 * 
	 * @param level
	 * @param jobs
	 * @param jobctrl
	 * @return
	 * @throws IOException
	 */
	private boolean whatDoWeDoNow(int[] level, List<ControlledJob> jobs, JobControl jobctrl) throws IOException{
		System.out.print("le job est fini.");
    	//comment savoir si il reste des frequents ?
    	long nbrOfFrequents = jobs.get(0).getJob().getCounters().findCounter(Counters.FREQ).getValue();
    	System.out.println("nombre de freq:"+nbrOfFrequents);
    	if(nbrOfFrequents>0){
    		level[0]++;
    		//rajoute des jobs, 
    		//this.addNewLevel(jobctrl.getSuccessfulJobList().size(), jobs);
    		this.addNewLevel(level, jobs);
//    		for(int i = 1; i < 3; i++) {
//    			this.addNewLevel(i, jobs);
//    		}
    		return true;
    	}
    	else{
    		//on a fini...
    		return false;
    	}
    	//si il en reste, alors on relance un nouveau niveau (gencand + mine)
    	//sinon, on a fini.
	}
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		AprioriMRDriver driver = new AprioriMRDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
