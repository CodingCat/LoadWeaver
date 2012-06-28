package workloadgen.loadjobs;

import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

/**
 * later, I will move the logic of job execution here 
 *
 */
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
