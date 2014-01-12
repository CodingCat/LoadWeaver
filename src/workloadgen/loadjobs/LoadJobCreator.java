package workloadgen.loadjobs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.RegexMapper;

import workloadgen.WorkloadRunner;
import workloadgen.utils.WorkloadGenConfParser;

public class LoadJobCreator extends GenericMRLoadGenerator{
	private static Configuration config = initConfig();
	private static FileSystem fs = initFs(config);
	private static boolean MULTIQUEUE = false;
	
	private static String USER_PREFIX = null;
	private static String INPUT_DATA_ROOT = null;
	private static String SMALL_INPUT_PATH = null;
	private static String MEDIUM_INPUT_PATH = null;
	private static String LARGE_INPUT_PATH = null;
	private static String GREP_SMALL_INPUT = null;
	private static String GREP_MEDIUM_INPUT = null;
	private static String GREP_LARGE_INPUT = null;
	private static String SORT_SMALL_INPUT = null;
	private static String SORT_MEDIUM_INPUT = null;
	private static String SORT_LARGE_INPUT = null;
	private static String WEB_SMALL_INPUT = null;
	private static String WEB_MEDIUM_INPUT = null;
	private static String WEB_LARGE_INPUT = null;
	
	private HashMap<String, Method> JobCreatingHandlers = new HashMap<String, Method>();

	public LoadJobCreator(String confPath) throws SecurityException, NoSuchMethodException{
		USER_PREFIX = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.user_prefix", "");
		INPUT_DATA_ROOT = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.root", "/workloadgen/data");
		SMALL_INPUT_PATH = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.smallpath", 
				 "part-000*0");
		MEDIUM_INPUT_PATH = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.meidumpath", 
				 "part-000*0,part-000*1,part-000*2");
		LARGE_INPUT_PATH = WorkloadGenConfParser.Instance(confPath).getString(
				"workloadgen.input.largepath", 
				 "*");
		GREP_SMALL_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/grep_data",
				SMALL_INPUT_PATH);
		GREP_MEDIUM_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/grep_data",
				MEDIUM_INPUT_PATH);
		GREP_LARGE_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/grep_data",
				LARGE_INPUT_PATH);
		SORT_SMALL_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/sort_data",
				SMALL_INPUT_PATH);
		SORT_MEDIUM_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/sort_data",
				MEDIUM_INPUT_PATH);
		SORT_LARGE_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/sort_data",
				LARGE_INPUT_PATH);
		WEB_SMALL_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/web_data",
				SMALL_INPUT_PATH);
		WEB_MEDIUM_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/web_data",
				MEDIUM_INPUT_PATH);
		WEB_LARGE_INPUT = getInputPath(USER_PREFIX + INPUT_DATA_ROOT + "/web_data",
				LARGE_INPUT_PATH);
		
		MULTIQUEUE = WorkloadGenConfParser.Instance(confPath).getBoolean(
				"workloadgen.system.multiqueue", false);
		registerJobCreatingHandlers();
	}
	
	private void registerJobCreatingHandlers() throws SecurityException, NoSuchMethodException{
		Class<?> [] parameters = {int.class, int.class, String.class, String.class, int.class};
		JobCreatingHandlers.put("Grep", 
				LoadJobCreator.class.getMethod("createGrep", parameters));
		JobCreatingHandlers.put("Sort", 
				LoadJobCreator.class.getMethod("createSort", parameters));
		JobCreatingHandlers.put("WebSort", 
				LoadJobCreator.class.getMethod("createWebSort", parameters));
	}
	
	private String getInputPath(String base, String filePaths){
		String [] files = filePaths.split(",");
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < files.length; i++){
			sb.append(base + "/" + files[i]).append(",");
		}
		return sb.substring(0, sb.length() - 1);
	}
	
	private JobConf setupWebSort(int numReducers, String size, String queue) 
			throws Exception{
		JobConf websortJob = new JobConf();
		String jobname = "web-sort-" + size;
		String inputDir = null;
		if (size.equals("small")){
			inputDir = WEB_SMALL_INPUT;
		}
		if (size.equals("medium")){
			inputDir = WEB_MEDIUM_INPUT;
		}
		if (size.equals("large")){
			inputDir = WEB_LARGE_INPUT;
		}
		Path outDir = new Path("web_out"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		
		websortJob.setJobName(jobname);
		websortJob.setMapperClass(SampleMapper.class);
		websortJob.setReducerClass(SampleReducer.class);
		websortJob.setJarByClass(GenericMRLoadGenerator.class);
		
		websortJob.setNumReduceTasks(numReducers);
		websortJob.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
		websortJob.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);

		websortJob.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		websortJob.setOutputValueClass(org.apache.hadoop.io.Text.class);
		
		FileInputFormat.addInputPaths(websortJob, inputDir);
        FileOutputFormat.setOutputPath(websortJob, outDir);

        websortJob.setCompressMapOutput(true);
        websortJob.setBoolean("mapred.output.compress", true);
        
        clearDir(outDir.toString());
		if (MULTIQUEUE == true){
			websortJob.set("mapred.fairscheduler.pool", queue);
		}
		
		return websortJob;
	}
	
	private JobConf setupSort(int numReducers, String size, String queue) 
			throws Exception{
		JobConf sortJob = new JobConf();
		String jobname = "java-sort-" + size;
		String inputDir = null;
		if (size.equals("small")){
			inputDir = SORT_SMALL_INPUT;
		}
		if (size.equals("medium")){
			inputDir = SORT_MEDIUM_INPUT;
		}
		if (size.equals("large")){
			inputDir = SORT_LARGE_INPUT;
		}
		Path outDir = new Path("sort_out"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		
		sortJob.setJobName(jobname);
		sortJob.setMapperClass(IdentityMapper.class);
		sortJob.setReducerClass(IdentityReducer.class);
		sortJob.setJarByClass(Sort.class);
		
		sortJob.setNumReduceTasks(numReducers);
		sortJob.setInputFormat(org.apache.hadoop.mapred.KeyValueTextInputFormat.class);
		sortJob.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

		sortJob.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		sortJob.setOutputValueClass(org.apache.hadoop.io.Text.class);
		
		FileInputFormat.addInputPaths(sortJob, inputDir);
        FileOutputFormat.setOutputPath(sortJob, outDir);

        clearDir(outDir.toString());
		if (MULTIQUEUE == true){
			sortJob.set("mapred.fairscheduler.pool", queue);
		}
		
		return sortJob;
	}
	
	private JobConf setupGrep(int numReducers, String size, String queue) 
			throws Exception{
		JobConf grepJob = new JobConf();
		String jobname = "grep-search-" + size;
		String inputDir = null;
		if (size.equals("small")){
			inputDir = GREP_SMALL_INPUT;
		}
		if (size.equals("medium")){
			inputDir = GREP_MEDIUM_INPUT;
		}
		if (size.equals("large")){
			inputDir = GREP_LARGE_INPUT;
		}
		Path outDir = new Path("grep_out"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		
		grepJob.setJobName(jobname);
		grepJob.setMapperClass(RegexMapper.class);
		grepJob.set("mapred.mapper.regex", "dfs[a-z.]+");

		grepJob.setCombinerClass(LongSumReducer.class);
		grepJob.setReducerClass(LongSumReducer.class);
		grepJob.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(grepJob, inputDir);
		FileOutputFormat.setOutputPath(grepJob, outDir);
        grepJob.setInputFormat(TextInputFormat.class);
        grepJob.setOutputFormat(SequenceFileOutputFormat.class);
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(LongWritable.class);
		clearDir(outDir.toString());
		if (MULTIQUEUE == true){
			grepJob.set("mapred.fairscheduler.pool", queue);
		}
		
		return grepJob;
	}
	
	public LoadJob createGrep(int numReducers, int timestamp, String size, String queue) throws Exception{
		JobConf grepjob = this.setupGrep(numReducers, size, queue);
		return new LoadJob(grepjob, timestamp);
	}
	
	public LoadJob createSort(int numReducers, int timestamp, String size, String queue) throws Exception{
		JobConf sortjob = this.setupSort(numReducers, size, queue);
		return new LoadJob(sortjob, timestamp);
	}
	
	public LoadJob createWebSort(int numReducers, int timestamp, String size, String queue) throws Exception{
		JobConf Websortjob = this.setupWebSort(numReducers, size, queue);
		return new LoadJob(Websortjob, timestamp);
	}
	
	private static Configuration initConfig(){
		Configuration conf = new Configuration();
		try {
			Path fileResource = new Path(WorkloadRunner.confPath);
			conf.addResource(fileResource);
		} catch (Exception e) {
			System.err.println("Error reading config file " + WorkloadRunner.confPath 
					+ ":"
					+ e.getMessage());
			return null;
		}
		return conf;
	}
	
	public LoadJob createJob(String jobtype, int numReducers, int timestamp, String inputSize,
                             String QueueName, int jobid)
			throws IllegalArgumentException, 
			IllegalAccessException, 
			InvocationTargetException, 
			SecurityException, 
			NoSuchMethodException{
		Method m = JobCreatingHandlers.get(jobtype);
		LoadJob newJob = (LoadJob) m.invoke(this, numReducers, timestamp, inputSize, QueueName, jobid);
		return newJob;
	}
	
	private static FileSystem initFs(Configuration conf){
		try{
			return FileSystem.get(conf);
		}
		catch(Exception e){
			System.out.println("init file system error");
			e.printStackTrace();
			return null;
		}
	}
	
	private static void clearDir(String dir){
		try{
			Path outfile = new Path(dir);
			System.out.println("deleting:" + outfile);
			fs.delete(outfile, true);
			if (!fs.exists(outfile)){
				System.out.println("Successfully Deleted");
			}
		}
		catch(Exception e){
			System.out.println("delete file error");
			e.printStackTrace();
			
		}
	}
	
}
