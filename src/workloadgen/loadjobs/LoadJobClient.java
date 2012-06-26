package workloadgen.loadjobs;

import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class LoadJobClient extends Thread{
	private JobControl loadengine = null;
	
	
	@Override
	public void run(){
		loadengine.run();
	}
}
