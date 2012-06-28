package workloadgen.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import workloadgen.loadjobs.LoadJobSubmissionPlan;

public class WorkloadGenTraceReplayer {
	
	private String tracePath = null;
	LoadJobSubmissionPlan subPlan = null;
	
	public WorkloadGenTraceReplayer(String tPath){
		this.tracePath = tPath;
		subPlan = new LoadJobSubmissionPlan();
	}
	
	public LoadJobSubmissionPlan getLoadTrace(){ 
		parse();
		return subPlan;
	}
	
	private void parse(){
		BufferedReader in;
		Pattern submitRecordPattern = Pattern.compile(
				"^(.+)\t([0-9]+)\t([0-9]+)\t([0-9]+)\t(.+)\t(.+)$");
		try {
			in = new BufferedReader(new FileReader(tracePath));
			String s;
			while ((s = in.readLine()) != null) {
				Matcher matcher = submitRecordPattern.matcher(s);
				if (matcher.find()) {
					for (int i = 0; i < Integer.parseInt(matcher.group(3)); i++)
					{
						LoadJobSubmissionPlan.LoadJobSubmissionPoint subPoint = 
								subPlan.new LoadJobSubmissionPoint(
										Integer.parseInt(matcher.group(2)),
										matcher.group(1),
										Integer.parseInt(matcher.group(3)));
						subPlan.addNewPoint(subPoint);
					}
				}
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
