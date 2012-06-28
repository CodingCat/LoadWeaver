package workloadgen.loadjobs;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

public class LoadJob extends Job {

	
	public LoadJob(JobConf jobConf) throws IOException {
		super(jobConf);
	}

}
