package workloadgen.loadjobs;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

public class LoadJob extends Job {

	private int submitTime = 0;
	
	public LoadJob(JobConf jobConf, int submitPoint) throws IOException {
		super(jobConf);
		this.submitTime = submitPoint;
	}
	
	public boolean checkSubmitState(int curTime){
		return (curTime >= submitTime);
	}
}
