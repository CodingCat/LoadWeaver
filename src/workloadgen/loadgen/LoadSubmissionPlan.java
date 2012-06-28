package workloadgen.loadgen;

import java.util.ArrayList;

/**
 * this class describe the trace of the workload
 */
public class LoadSubmissionPlan {

	enum LoadJobType{
		webdataScan,
		webdataSort
	};
	
	/**
	 * this class describe each job_submission event in trace
	 */
	public class LoadSubmissionPoint{
		
		private LoadJobType jobType;
		private int subTime;
		private int numOfJobs;
		private int numOfReduce;
		
		public LoadSubmissionPoint(int timestamp, String type, int numJobs, int numReduce){
			subTime = timestamp;
			jobType = LoadJobType.valueOf(type);
			numOfJobs = numJobs;
			numOfReduce = numReduce;
		}
		
		public int getTimestamp(){
			return subTime;
		}
		
		public int getNumOfJobs(){
			return this.numOfJobs;
		}
		
		public int getNumReduce(){
			return this.numOfReduce;
		}
		
		/**
		 * for test
		 */
		public void dump(){
			System.out.println("at " + subTime + " submit " + jobType);
		}
	}
	
	private ArrayList<LoadSubmissionPoint> list = null;  
	
	public LoadSubmissionPlan(){
		list = new ArrayList<LoadSubmissionPoint>();
	}
	
	public void addNewPoint(LoadSubmissionPoint point){
		list.add(point);
	}
	
	public ArrayList<LoadSubmissionPoint> getList(){
		return this.list;
	}
	
	/**
	 * for test
	 */
	public void dump(){
		for (int i = 0; i < list.size(); i++){
			list.get(i).dump();
		}
	}
}
