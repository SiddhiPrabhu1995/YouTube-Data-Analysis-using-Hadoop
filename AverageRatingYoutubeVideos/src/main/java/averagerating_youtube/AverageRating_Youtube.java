package averagerating_youtube;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;


public class AverageRating_Youtube  {

    /**
     * @param args the command line arguments
     */
    //@Override
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageRating_Youtube");
		//Job job = new Job(getConf());
        //job.setJobName("AverageRating_Youtube");

        job.setJarByClass(AverageRating_Youtube.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AvgRating_CommCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AverageRating_CommentCountTuple.class);
        job.setCombinerClass(AvgRating_CommCountCombiner.class);
        job.setReducerClass(AvgRating_CommCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AverageRating_CommentCountTuple.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }


}
