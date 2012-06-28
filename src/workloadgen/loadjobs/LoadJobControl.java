package workloadgen.loadjobs;

import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class LoadJobControl extends JobControl {

	public LoadJobControl(String groupName) {
		super(groupName);
	}

	@Override
	public String addJob(Job job){
		super.addJob(job);
		return job.getJobID();
	}
	
}
