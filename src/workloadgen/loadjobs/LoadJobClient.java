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
	
	private static String USER_PREFIX = null;
	private static String INPUT_DATA_ROOT = null;
	
	private static String GREP_SMALL_INPUT = null;

	private static String SMALL_INPUT_PATH = null;
	
	public LoadJobClient(String conf, String trace, LoadJobController controller){
		this.confPath = conf;
		this.tracePath = trace;
		this.loadcontroller = controller;
		init();
	}
	
	private void init(){
		traceGenerator = new LoadTraceReplayer(this.tracePath);
		loadcreator = new LoadJobCreator();
		USER_PREFIX = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.user_prefix", "");
		INPUT_DATA_ROOT = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.root", "/workloadgen/data");
		SMALL_INPUT_PATH = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.smallpath", 
				 "part-00000,part-00001,part-00002");
		GREP_SMALL_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/grep_data",
				SMALL_INPUT_PATH);
	}
	
	private String getInputPath(String base, String filePaths){
		String [] files = filePaths.split(",");
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < files.length; i++){
			sb.append(base + "/" + files[i]).append(",");
		}
		return sb.substring(0, sb.length() - 1);
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
				LoadJob job = loadcreator.createGrep(GREP_SMALL_INPUT, 
						"grep_out", 
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
