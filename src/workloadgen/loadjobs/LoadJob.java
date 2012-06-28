package workloadgen.loadjobs;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

public class LoadJob extends Job {

	private int state;
	int submitTime = 0;
	
	public LoadJob(JobConf jobConf, int submitPoint) throws IOException {
		super(jobConf);
		this.submitTime = submitPoint;
		this.setJobName(jobConf.getJobName());
	}
	
	public boolean checkSubmitState(int curTime){
		//System.out.println("time:" + curTime + " submitTime:" + submitTime);
		return (curTime >= submitTime);
	}
	
	@Override
	public void setState(int s){
		this.state = s;
	}
	
	public int getState(){
		return this.state;
	}
	
	@Override
	public String toString(){
		StringBuffer ret = new StringBuffer(super.toString());
		ret.append("job Mapper:" + this.getJobConf().getMapperClass().toString()).append("\n");
		ret.append("job Reducer:" + this.getJobConf().getReducerClass().toString()).append("\n");
		return ret.toString();
	}
}
