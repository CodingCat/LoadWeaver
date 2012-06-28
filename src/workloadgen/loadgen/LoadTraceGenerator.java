package workloadgen.loadgen;

/**
 * the abstract class for generic trace generator
 * in our system, generator can be Tracereplayer and Distribution-based Generator
 */
public abstract class LoadTraceGenerator {

	public abstract LoadSubmissionPlan getSubmissionPlan(); 
}
