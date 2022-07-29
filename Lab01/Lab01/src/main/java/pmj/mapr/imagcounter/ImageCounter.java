package pmj.mapr.imagcounter;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class ImageCounter {

	public static void main(String[] args) throws Exception,IOException {

		String input = "d:/programs-22a/data/mr/webaccess/access_log.txt";
		String output = "d:/programs-22a/output/mr/imagecounter";
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"Image Counter");
		job.setJarByClass(ImageCounter.class);
		job.setMapperClass(ImageCounterMapper.class);
		job.setReducerClass(ImageCounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outpath=new Path(output);
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job,outpath);
		outpath.getFileSystem(conf).delete(outpath,true);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
