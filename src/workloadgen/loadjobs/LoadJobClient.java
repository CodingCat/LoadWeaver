package workloadgen.loadjobs;

import java.io.IOException;

import workloadgen.loadgen.LoadSubmissionPlan;
import workloadgen.loadgen.LoadTraceGenerator;
import workloadgen.loadgen.trace.LoadTraceReplayer;
import workloadgen.utils.WorkloadGenConfParser;

public class LoadJobClient {
	
	private LoadJobController loadcontroller= null;
	private LoadTraceGenerator traceGenerator= null;
	private LoadJobCreator loadcreator = null;
	private String confPath = null;
	private String tracePath = null;
	
	private static String GRID_MIX_DATA = null;
	private static String WEBJOB_INPUT = null;

	private static String SMALL_INPUT_PATH = null;
	//private static String MEDIUM_INPUT_PATH = null;
	//private static String LARGE_INPUT_PATH = null;
	private int totalSubmission = 0;
	
	public LoadJobClient(String conf, String trace, LoadJobController controller){
		this.confPath = conf;
		this.tracePath = trace;
		this.loadcontroller = controller;
		init();
	}
	
	private void init(){
		traceGenerator = new LoadTraceReplayer(this.tracePath);
		loadcreator = new LoadJobCreator();
		GRID_MIX_DATA = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.root", "/user/zhunan/gridmix/data");
		SMALL_INPUT_PATH = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.smallpath", 
				 "part-00000");
		WEBJOB_INPUT = GRID_MIX_DATA + "/test/";
	}
	
	public void addAllJobs() throws Exception{
		int lasttimestamp = 0;
		LoadSubmissionPlan plan = traceGenerator.getSubmissionPlan();
		System.out.println("plan length:" + plan.getList().size());
		for (LoadSubmissionPlan.LoadSubmissionPoint subpoint : plan.getList()) {
			if (subpoint.getTimestamp() < lasttimestamp) {
				System.out.println("submit records must be sorted by time with asc order");
				continue;
			}
			// create a job according to subpoint
			try{
				/*LoadJob job = loadcreator.createWebdataScan(
						WEBJOB_INPUT + SMALL_INPUT_PATH,
						"out/webdatascan-small-out-" + (++this.totalSubmission),
						subpoint.getNumReduce(),
						subpoint.getTimestamp());*/
				LoadJob job = loadcreator.createGrep("input", 
						"out", 
						subpoint.getNumReduce(), 
						"dfs[a-z.]+",
						subpoint.getTimestamp());
				loadcontroller.addJob(job);
				lasttimestamp = subpoint.getTimestamp();
			}
			catch (IOException e){
				e.printStackTrace();
			}
			
		}
		
	}
}
