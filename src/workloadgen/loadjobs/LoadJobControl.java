package workloadgen.loadjobs;


import java.util.ArrayList;

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
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		ArrayList<Job> arrlist = this.getWaitingJobs();
		for (int i = 0; i < arrlist.size(); i++){
			sb.append(arrlist.get(i).getJobID()).append("\n");
		}
		return sb.toString();
	}
}
