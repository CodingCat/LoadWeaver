package workloadgen.loadjobs;

import java.util.ArrayList;

public class LoadJobSubmissionPlan {

	enum LoadJobType{
		webdataScan,
		webdataSort
	};
	
	public class LoadJobSubmissionPoint{
		
		private LoadJobType jobType;
		private int subTime;
		private int numOfJobs;
		
		public LoadJobSubmissionPoint(int timestamp, String type, int numJobs){
			subTime = timestamp;
			jobType = LoadJobType.valueOf(type);
			numOfJobs = numJobs;
		}
		
		public int getTimestamp(){
			return subTime;
		}
		
		public int getNumOfJobs(){
			return this.numOfJobs;
		}
		
		public void dump(){
			System.out.println("at " + subTime + " submit " + jobType);
		}
	}
	
	private ArrayList<LoadJobSubmissionPoint> list = null;  
	private int curIndex = 0;
	
	public LoadJobSubmissionPlan(){
		list = new ArrayList<LoadJobSubmissionPoint>();
	}
	
	public void addNewPoint(LoadJobSubmissionPoint point){
		list.add(point);
	}
	
	public void removePoint(LoadJobSubmissionPoint point){
		list.remove(point);
	}
	
	public LoadJobSubmissionPoint getNextSubOpt(){ 
		return list.get(curIndex++);
	}
	
	public ArrayList<LoadJobSubmissionPoint> getList(){
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
