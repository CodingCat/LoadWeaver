package workloadgen.loadjobs;

import java.io.IOException;
import java.util.Calendar;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.GenericMRLoadGenerator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.lib.RegexMapper;

import workloadgen.WorkloadRunner;

/**
 * create a concrete job 
 *
 */
public class LoadJobCreator extends GenericMRLoadGenerator{
	private static Configuration config = initConfig();
	private static FileSystem fs = initFs(config);
	
	public LoadJobCreator(){
	
	}
	
	private JobConf setupGrep(String indir, String outdir, int numReducers, String regPattern) 
			throws Exception{
		JobConf grepJob = new JobConf();
		grepJob.setJobName("grep-search");
		Path tempDir = new Path("grep-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileInputFormat.setInputPaths(grepJob, indir);

		grepJob.setMapperClass(RegexMapper.class);
		grepJob.set("mapred.mapper.regex", regPattern);

		grepJob.setCombinerClass(LongSumReducer.class);
		grepJob.setReducerClass(LongSumReducer.class);
		grepJob.setNumReduceTasks(numReducers);

		FileOutputFormat.setOutputPath(grepJob, tempDir);
		grepJob.setOutputFormat(SequenceFileOutputFormat.class);
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(LongWritable.class);
		fs.delete(tempDir, true);
		return grepJob;
	}
	
	public LoadJob createGrep(String indir, String outdir, int numReducers, 
			String regPattern,
			int timestamp) throws Exception{
		JobConf grepjob = this.setupGrep(indir, outdir, numReducers, regPattern);
		return new LoadJob(grepjob, timestamp);
	}
	
	private JobConf setupWebdataScan(String indir, String outdir, int numReducers) throws Exception{
		//TODO:support multiple kinds of sizes
		StringBuffer sb = new StringBuffer();
		sb.append("-keepmap 0.2 ");
		sb.append("-keepred 5 ");
		sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
		sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
		sb.append("-outKey org.apache.hadoop.io.Text ");
		sb.append("-outValue org.apache.hadoop.io.Text ");
		sb.append("-indir ").append(indir).append(" ");
		sb.append("-outdir ").append(outdir).append(" ");
		sb.append("-r ").append(numReducers);

		String[] args = sb.toString().split(" ");
		clearDir(outdir);
		
		JobConf job = new JobConf();
		job.setJobName("WebdataScan." + "small");
	    job.setJarByClass(GenericMRLoadGenerator.class);
	    job.setMapperClass(SampleMapper.class);
	    job.setReducerClass(SampleReducer.class);
	    if (!parseArgs(args, job)) {
	      return null;
	    }
		
		return job;
	}
	
	
	/**
	 * create a WebdataScan job 
	 * @param argv, the arguments to run jobs 
	 * @return the JobConf object which describe the job
	 * @throws IOException
	 */
	public LoadJob createWebdataScan(String indir, 
			String outdir, 
			int numReducers, 
			int timestamp)
			throws Exception {
		
		outdir += String.valueOf(Calendar.getInstance().getTime().getTime());
		JobConf job = setupWebdataScan(indir, outdir, numReducers);
		
	    if (null == FileOutputFormat.getOutputPath(job)) {
	      // No output dir? No writes
	      job.setOutputFormat(NullOutputFormat.class);
	    }

	    if (0 == FileInputFormat.getInputPaths(job).length) {
	      // No input dir? Generate random data
	      System.err.println("No input path; ignoring InputFormat");
	      confRandom(job);
	    } else if (null != job.getClass("mapred.indirect.input.format", null)) {
	      // specified IndirectInputFormat? Build src list
	      JobClient jClient = new JobClient(job);
	      Path tmpDir = new Path(jClient.getFs().getHomeDirectory(), ".staging");
	      Random r = new Random();
	      Path indirInputFile = new Path(tmpDir,
	          Integer.toString(r.nextInt(Integer.MAX_VALUE), 36) + "_files");
	      job.set("mapred.indirect.input.file", indirInputFile.toString());
	      SequenceFile.Writer writer = SequenceFile.createWriter(
	          tmpDir.getFileSystem(job), job, indirInputFile,
	          LongWritable.class, Text.class,
	          SequenceFile.CompressionType.NONE);
	      try {
	        for (Path p : FileInputFormat.getInputPaths(job)) {
	          FileSystem fs = p.getFileSystem(job);
	          Stack<Path> pathstack = new Stack<Path>();
	          pathstack.push(p);
	          while (!pathstack.empty()) {
	            for (FileStatus stat : fs.listStatus(pathstack.pop())) {
	              if (stat.isDir()) {
	                if (!stat.getPath().getName().startsWith("_")) {
	                  pathstack.push(stat.getPath());
	                }
	              } else {
	                writer.sync();
	                writer.append(new LongWritable(stat.getLen()),
	                    new Text(stat.getPath().toUri().toString()));
	              }
	            }
	          }
	        }
	      } finally {
	        writer.close();
	      }
	    }
		return new LoadJob(job, timestamp);
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
