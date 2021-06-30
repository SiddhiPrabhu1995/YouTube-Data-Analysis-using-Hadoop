package top_viewed_video;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private Text video_name = new Text();
    private FloatWritable views = new FloatWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String str[] = line.split(",");

        if (str.length >= 5) {
            video_name.set(str[0]);
               try {
            	   float temp = Float.parseFloat(str[5]); //typecasting string to Integer
            	   views.set(temp);
            	   context.write(video_name, views);
               }catch(Exception e){
            	   e.printStackTrace();
               }
               
                //views.set(temp);
            }                  
        
    }

}

