package workloadgen.loadgen.trace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import workloadgen.loadgen.LoadSubmissionPlan;
import workloadgen.loadgen.LoadTraceGenerator;


public class LoadTraceReplayer extends LoadTraceGenerator{
	
	private String tracePath = null;
	LoadSubmissionPlan subPlan = null;
	
	public LoadTraceReplayer(String tPath){
		this.tracePath = tPath;
		subPlan = new LoadSubmissionPlan();
	}
	
	@Override
	public LoadSubmissionPlan getSubmissionPlan(){ 
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
						LoadSubmissionPlan.LoadSubmissionPoint subPoint = 
								subPlan.new LoadSubmissionPoint(
										matcher.group(1),
										Integer.parseInt(matcher.group(2)),
										Integer.parseInt(matcher.group(3)),
										Integer.parseInt(matcher.group(4)),
										matcher.group(5),
										matcher.group(6));
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
