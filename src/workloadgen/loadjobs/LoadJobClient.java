package workloadgen.loadjobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.Job;

import workloadgen.loadgen.LoadSubmissionPlan;
import workloadgen.loadgen.LoadTraceGenerator;
import workloadgen.loadgen.trace.LoadTraceReplayer;
import workloadgen.utils.WorkloadGenConfParser;

public class LoadJobClient extends Thread{
	
	private LoadJobControl loadengine = null;
	private LoadTraceGenerator traceGenerator= null;
	private LoadJobCreator loadcreator = null;
	private Configuration config = null; //= initConfig();
	private FileSystem fs = null; //= initFs();
	private String confPath = null;
	private String tracePath = null;
	
	private static String GRID_MIX_DATA = null;
	private static String WEBJOB_INPUT = null;

	private static String SMALL_INPUT_PATH = null;
	//private static String MEDIUM_INPUT_PATH = null;
	//private static String LARGE_INPUT_PATH = null;
	
	public LoadJobClient(String conf, String trace){
		this.confPath = conf;
		this.tracePath = trace;
		init();
	}
	
	private void init(){
		traceGenerator = new LoadTraceReplayer(this.tracePath);
		loadengine = new LoadJobControl("WorkloadGen");
		loadcreator = new LoadJobCreator();
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

	@Override
	public void run(){
		try {
			int lasttimestamp = 0;
			LoadSubmissionPlan plan = traceGenerator.getSubmissionPlan();
		//	Thread LoadJobEngine = new Thread(loadengine);
		//	LoadJobEngine.start();
			for (LoadSubmissionPlan.LoadSubmissionPoint subpoint : 
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
				try {
					loadengine.addJob(new Job(loadcreator.createWebdataScan(
							WEBJOB_INPUT + SMALL_INPUT_PATH,
							"out/webdatascan-small-out", 
							subpoint.getNumReduce())));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("add new job");
				lasttimestamp = subpoint.getTimestamp();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
