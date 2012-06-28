package workloadgen.loadjobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import workloadgen.loadgen.LoadSubmissionPlan;
import workloadgen.loadgen.LoadTraceGenerator;
import workloadgen.loadgen.trace.LoadTraceReplayer;
import workloadgen.utils.WorkloadGenConfParser;

public class LoadJobClient {
	
	private LoadJobController loadcontroller= null;
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
	
	public LoadJobClient(String conf, String trace, LoadJobController controller){
		this.confPath = conf;
		this.tracePath = trace;
		this.loadcontroller = controller;
		init();
	}
	
	private void init(){
		traceGenerator = new LoadTraceReplayer(this.tracePath);
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

	
	public void addAllJobs(){
		int lasttimestamp = 0;
		LoadSubmissionPlan plan = traceGenerator.getSubmissionPlan();
		for (LoadSubmissionPlan.LoadSubmissionPoint subpoint : plan.getList()) {
			if (subpoint.getTimestamp() < lasttimestamp) {
				System.out.println("submit records must be sorted by time with asc order");
				continue;
			}
			// create a job according to subpoint
			try{
				loadcontroller.addJob(loadcreator.createWebdataScan(
						WEBJOB_INPUT + SMALL_INPUT_PATH,
						"out/webdatascan-small-out",
						subpoint.getNumReduce(),
						subpoint.getTimestamp()));
				lasttimestamp = subpoint.getTimestamp();
			}
			catch (IOException e){
				e.printStackTrace();
			}
			
		}
		
	}
}
