package workloadgen;

import workloadgen.loadjobs.LoadJobClient;


public class WorkloadRunner {
	
	private LoadJobClient client = null;
	private static WorkloadRunner _instance = null;
	
	public WorkloadRunner(String conf, String trace){	
		client = new LoadJobClient(conf, trace);
	}
	
	/**
	 * the main loop 
	 */
	public void mainService(){
		client.start();
	}
	
	/**
	 * construct the unique instance of WorkloadRunner
	 * @param conf the configuration file path
	 * @param trace the trace file
	 * @return the instance of WorkloadRunner
	 */
	private static WorkloadRunner Instance(String conf, String trace){
		if (_instance == null){
			_instance = new WorkloadRunner(conf, trace);
		}
		return _instance;
	}
	
	/**
	 * @param args[0] confPath
	 * @param args[1] tracePath
	 */
	public static void main(String[] args) {
		WorkloadRunner.Instance(args[0], args[1]).mainService();
	}

}
