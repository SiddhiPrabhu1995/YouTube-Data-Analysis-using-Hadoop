package top_viewed_video;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	static float max = 0;
    static int sum = 0;
    static String finalKey = "";

	@Override	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        for (FloatWritable val : values) {
            sum += val.get();
        }
        if (sum > max) {
        	max = sum;
        	finalKey = key.toString();
        }
    }
	
	
	@Override
	protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
	    context.write(new Text(finalKey), new FloatWritable(max));
	    	// TODO Auto-generated method stub
	
	}
	
	
}
