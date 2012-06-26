package workloadgen;

import workloadgen.loadjobs.LoadJobClient;


public class WorkloadRunner {
	
	private LoadJobClient client = null;
	static WorkloadRunner _instance = null;
	
	public WorkloadRunner(){	
	
	}
	
	public void mainService(){
		client.start();
	}
	
	static WorkloadRunner Instance(){
		if (_instance == null){
			_instance = new WorkloadRunner();
		}
		return _instance;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		WorkloadRunner.Instance().mainService();
	}

}
