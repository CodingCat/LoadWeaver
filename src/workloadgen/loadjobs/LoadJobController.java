package workloadgen.loadjobs;

import workloadgen.utils.LoadJobQueue;


/**
 * later, I will move the logic of job execution here 
 *
 */
public class LoadJobController implements Runnable{

	//I have to set these variables, since in JobControl, the runnerState is private
	// The thread can be in one of the following state
	
	private enum JobControllerState{
		RUNNING,
		SUSPENDED,
		STOPPED,
		STOPPING,
		READY
	}
	
	private JobControllerState controllerState;
	private int suspendDuration = 0;
	private int currentTime = 0;
	
	private LoadJobQueue<LoadJob> waitingQueue;
	private LoadJobQueue<LoadJob> readyQueue;
	private LoadJobQueue<LoadJob> runningQueue;
	private LoadJobQueue<LoadJob> successfulQueue;
	private LoadJobQueue<LoadJob> failedQueue;
	
	private long nextJobID;
	private String groupName;
	
	
	public LoadJobController(String gName) {
		this.waitingQueue = new LoadJobQueue<LoadJob>();
	    this.readyQueue = new LoadJobQueue<LoadJob>();
	    this.runningQueue = new LoadJobQueue<LoadJob>();
	    this.successfulQueue = new LoadJobQueue<LoadJob>();
	    this.failedQueue = new LoadJobQueue<LoadJob>();
		controllerState = JobControllerState.READY;
		currentTime = 0;
		groupName = gName;
	}
	
	private String getNextJobID() {
		nextJobID += 1;
		return this.groupName + this.nextJobID;
	}

	public String addJob(LoadJob job){
		String id = this.getNextJobID();
	    job.setJobID(id);
	    job.setState(LoadJob.WAITING);
	    this.addToQueue(job);
		return job.getJobID();
	}
	
	public void suspend(int duration){
		if (this.controllerState == JobControllerState.RUNNING) {
			this.controllerState = JobControllerState.SUSPENDED;
		}
		this.suspendDuration = duration;
	}
	
	public void stop(){
		this.controllerState = JobControllerState.STOPPING;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for (LoadJob job: waitingQueue){
			sb.append(job).append("\n");
		}
		return sb.toString();
	}
	
	private static void addToQueue(LoadJob job, LoadJobQueue<LoadJob> queue) {
		synchronized (queue) {
			queue.add(job);
		}
	}
	
	private void addToQueue(LoadJob job) {
		LoadJobQueue<LoadJob> queue = getQueue(job.getState());
		addToQueue(job, queue);
	}
	
	private LoadJobQueue<LoadJob> getQueue(int state) {
		LoadJobQueue<LoadJob> retv = null;
		if (state == LoadJob.WAITING) {
			retv = this.waitingQueue;
		} else if (state == LoadJob.READY) {
			retv = this.readyQueue;
		} else if (state == LoadJob.RUNNING) {
			retv = this.runningQueue;
		} else if (state == LoadJob.SUCCESS) {
			retv = this.successfulQueue;
		} else if (state == LoadJob.FAILED || state == LoadJob.DEPENDENT_FAILED) {
			retv = this.failedQueue;
		}
		return retv;
	}
	
	synchronized public boolean allFinished() {
		return this.waitingQueue.size() == 0 && this.readyQueue.size() == 0
				&& this.runningQueue.size() == 0;
	}

	private void checkWaitingJobs(){
		LoadJobQueue<LoadJob> oldjobs = waitingQueue;
		waitingQueue = new LoadJobQueue<LoadJob>();
		for (LoadJob job : oldjobs){
			if (job.checkSubmitState(currentTime)){
				System.out.println(job.getJobID() + " is ready");
				job.setState(LoadJob.READY);
			}
			else{
				this.suspendDuration = job.submitTime - currentTime;
			}
			this.addToQueue(job);
		}
	}
	
	private void checkRunningJobs(){
		//TODO:finish all
		LoadJobQueue<LoadJob> oldjobs = runningQueue;
		runningQueue = new LoadJobQueue<LoadJob>();
		for(LoadJob job : oldjobs){
			job.setState(LoadJob.SUCCESS);
			addToQueue(job);
		}
	}
	
	private void startReadyJobs(){
		LoadJobQueue<LoadJob> oldjobs = readyQueue;
		readyQueue = new LoadJobQueue<LoadJob>();
		for (LoadJob job: oldjobs){
			job.setState(LoadJob.RUNNING);
			addToQueue(job);
		}
	}
	
	@Override
	public void run() {
		this.controllerState = JobControllerState.RUNNING;
		while (this.controllerState != JobControllerState.STOPPING) {
			while (this.controllerState == JobControllerState.SUSPENDED) {
				try {
					Thread.sleep(suspendDuration);
					if (this.controllerState == JobControllerState.SUSPENDED){
						this.controllerState = JobControllerState.RUNNING;
					}
					currentTime += suspendDuration;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			checkRunningJobs();
			checkWaitingJobs();
			startReadyJobs();
			if (this.allFinished()){
				this.stop();
			}
			else{
				this.controllerState = JobControllerState.SUSPENDED;
			}
			if (this.controllerState != JobControllerState.RUNNING
					&& this.controllerState != JobControllerState.SUSPENDED) {
				break;
			}
		}
		this.controllerState = JobControllerState.STOPPED;
	}
}
