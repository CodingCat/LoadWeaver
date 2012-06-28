package workloadgen;

import workloadgen.loadjobs.LoadJobClient;


public class WorkloadRunner {
	
	private LoadJobClient client = null;
	static WorkloadRunner _instance = null;
	
	public WorkloadRunner(String conf, String trace){	
		client = new LoadJobClient(conf, trace);
	}
	
	public void mainService(){
		client.start();
	}
	
	static WorkloadRunner Instance(String conf, String trace){
		if (_instance == null){
			_instance = new WorkloadRunner(conf, trace);
		}
		return _instance;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		WorkloadRunner.Instance(args[0], args[1]).mainService();
	}

}
