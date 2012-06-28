package workloadgen.loadjobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.Job;

import workloadgen.utils.WorkloadGenConfParser;
import workloadgen.utils.WorkloadGenTraceReplayer;

public class LoadJobClient extends Thread{
	
	private LoadJobControl loadengine = null;
	private WorkloadGenTraceReplayer traceReplayer= null;
	private LoadJobCreator loadcreator = null;
	private Configuration config = null; //= initConfig();
	private FileSystem fs = null; //= initFs();
	private String confPath = null;
	private String tracePath = null;
	// we use gridmix2's jobs
	// thanks, unknown authors
	private static String GRID_MIX_DATA = null;
	private static String WEBJOB_INPUT = null;

	private static final int NUM_OF_REDUCES_FOR_SMALL_JOB = 15;
	//private static final int NUM_OF_REDUCES_FOR_MEDIUM_JOB = 15;
	//private static final int NUM_OF_REDUCES_FOR_LARGE_JOB = 15;

	private static String SMALL_INPUT_PATH = null;
	//private static String MEDIUM_INPUT_PATH = null;
	//private static String LARGE_INPUT_PATH = null;
	
	public LoadJobClient(String conf, String trace){
		this.confPath = conf;
		this.tracePath = trace;
		init();
	}
	
	private void init(){
		traceReplayer = new WorkloadGenTraceReplayer(this.tracePath);
		loadengine = new LoadJobControl("WorkloadGen");
		loadcreator = new LoadJobCreator(confPath);
		config = initConfig();
		fs = initFs();
		GRID_MIX_DATA = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.root", "/gridmix/data");
		SMALL_INPUT_PATH = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.smallpath", 
				 "{part-00000,part-00001,part-00002}");
		WEBJOB_INPUT = GRID_MIX_DATA + "/WebSimulationBlockCompressed";
	}
	
	private void clearDir(String dir) {
		try {
			Path outfile = new Path(dir);
			fs.delete(outfile, true);
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("delete file error:");
			System.out.println(ex.toString());
		}
	}
	
	private FileSystem initFs() {
		try {
			return FileSystem.get(config);
		} catch (Exception e) {
			System.out.println("fs initation error: " + e.getMessage());
		}
		return null;
	}
	
	private Configuration initConfig() {
		Configuration conf = new Configuration();
		try {
			Path fileResource = new Path(this.confPath);
			conf.addResource(fileResource);
		} catch (Exception e) {
			System.err.println("Error reading config file " + this.confPath + ":"
					+ e.getMessage());
			return null;
		}
		return conf;
	}

	private String [] setupWebdataScan(){
		//TODO:support multiple kinds of sizes
		final String indir = WEBJOB_INPUT + SMALL_INPUT_PATH;
		final String outdir = "out/webdatascan-small-out";
		int numReducers = NUM_OF_REDUCES_FOR_SMALL_JOB;
		StringBuffer sb = new StringBuffer();
		sb.append("-keepmap 0.2 ");
		sb.append("-keepred 5 ");
		sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
		sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
		sb.append("-outKey org.apache.hadoop.io.Text ");
		sb.append("-outValue org.apache.hadoop.io.Text ");
		sb.append("-indir ").append(indir).append(" ");
		sb.append("-outdir ").append(outdir).append(" ");
		sb.append("-r ").append(numReducers);

		String[] args = sb.toString().split(" ");
	//	clearDir(outdir);
		
		return args;
	}
	
	@Override
	public void run(){
		try {
			int lasttimestamp = 0;
			LoadJobSubmissionPlan plan = traceReplayer.getLoadTrace();
		//	Thread LoadJobEngine = new Thread(loadengine);
		//	LoadJobEngine.start();
			for (LoadJobSubmissionPlan.LoadJobSubmissionPoint subpoint : 
					plan.getList()) {
				if (subpoint.getTimestamp() < lasttimestamp) {
					System.out.println("submit records must be sorted by time with asc order");
					continue;
				}
				if (subpoint.getTimestamp() > lasttimestamp) {
					//we have submitted all required job at current timestamp;
					//let's start bunch of jobs
		//			loadengine.suspend();
					System.out.println("let's sleep for " + 
							(subpoint.getTimestamp() - lasttimestamp) + " seconds");
					sleep((subpoint.getTimestamp() - lasttimestamp) * 1000);
				}
				//create a job according to subpoint
				//loadengine.resume();
				if (lasttimestamp != 0){
					System.out.println("resume loadengine");
				}
				//loadengine.addJob(new Job(loadcreator.createWebdataScan(setupWebdataScan())));
				System.out.println("add new job");
				lasttimestamp = subpoint.getTimestamp();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
