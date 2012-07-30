package workloadgen.loadjobs;

import workloadgen.loadgen.LoadSubmissionPlan;
import workloadgen.loadgen.LoadTraceGenerator;
import workloadgen.loadgen.trace.LoadTraceReplayer;

public class LoadJobClient {
	
	private LoadJobController loadcontroller= null;
	private LoadTraceGenerator traceGenerator= null;
	private LoadJobCreator loadcreator = null;
	private String confPath = null;
	private String tracePath = null;
	
		
	public LoadJobClient(String conf, String trace, LoadJobController controller) {
		try {
			this.confPath = conf;
			this.tracePath = trace;
			this.loadcontroller = controller;
			init();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void init() throws SecurityException, NoSuchMethodException{
		traceGenerator = new LoadTraceReplayer(this.tracePath);
		loadcreator = new LoadJobCreator(confPath);
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
				LoadJob job  = loadcreator.createJob(
						subpoint.getJobType(),
						subpoint.getNumReduce(), 
						subpoint.getTimestamp(),
						subpoint.getInputSize(),
						subpoint.getQueueName());
				if (job == null){
					throw new Exception("unrecognized job type");
				}
				loadcontroller.addJob(job);
				lasttimestamp = subpoint.getTimestamp();
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		
	}
}
