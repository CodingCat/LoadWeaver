package workloadgen.loadjobs;

public class LoadJobSubmissionPlan {

	enum LoadJobType{
		Sort, WebScan;
	};
	
	public class LoadJobSubmissionPoint{
		
		LoadJobType jobType;
		double subTime;
		
		public LoadJobSubmissionPoint(double timestamp, LoadJobType type){
			subTime = timestamp;
			jobType = type;
		}
	}
	
}
