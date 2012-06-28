package workloadgen.loadjobs;

import java.io.IOException;
import java.util.Random;
import java.util.Stack;

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
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class LoadJobCreator extends GenericMRLoadGenerator{
	
	public LoadJobCreator(String conf){
	
	}
	
	/**
	 *  
	 * @param argv
	 * @return
	 * @throws IOException
	 */
	public JobConf createWebdataScan(String [] argv) throws IOException {
		JobConf job = new JobConf();
	    job.setJarByClass(GenericMRLoadGenerator.class);
	    job.setMapperClass(SampleMapper.class);
	    job.setReducerClass(SampleReducer.class);
	    if (!parseArgs(argv, job)) {
	      return null;
	    }

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
		return job;
	}
}