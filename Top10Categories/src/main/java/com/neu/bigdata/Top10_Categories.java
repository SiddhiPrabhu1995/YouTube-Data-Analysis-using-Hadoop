/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10_Categories {

    public static class Map1 extends Mapper<Object, Text, Text, FloatWritable> {

        private FloatWritable video_rating = new FloatWritable();
        private Text video_id = new Text();

        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            video_id = new Text(fields[0]);
            try {
            if (fields.length > 6) {
            //if (!fields[6].isEmpty()) {
                video_rating = new FloatWritable(Float.parseFloat(fields[6]));
            }

            context.write(video_id, video_rating);
            }catch(Exception e) {
            	e.printStackTrace();
            }
        }

    }

    public static class Reduce1 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            float sum = 0, avg = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                ++count;
            }

            avg = sum / count;
            result.set(avg);
            context.write(key, result);
        }

    }

    public static class Map2 extends Mapper<Object, Text, FloatWritable, Text> {

        @Override
        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
          

            String row[] = value.toString().split("\\t");
            Text video_id = new Text(row[0]);
            String rating = row[1];

            try {
                FloatWritable ratingg = new FloatWritable(Float.parseFloat(rating));
                context.write(ratingg, video_id);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public static class Reduce2 extends Reducer<FloatWritable, Text, Text, FloatWritable> {

        private static int count = 25;

        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                if (count > 0) {
                    context.write(val, key);
                    --count;
                } else {
                    break;
                }
            }

        }

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf1 = new Configuration();
         Configuration conf = new Configuration();
     
        Job job1 = Job.getInstance(conf1, "Chaining");
        job1.setJarByClass(Top10_Categories.class);

        job1.setMapperClass(Map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);

        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setCombinerClass(Reduce1.class);

        
        FileInputFormat.addInputPath(job1,new Path(args[0]));
         
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean complete = job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Chaining");

        if (complete) {

            job2.setJarByClass(Top10_Categories.class);
            job2.setMapperClass(Map2.class);
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setReducerClass(Reduce2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(FloatWritable.class);

            job2.setSortComparatorClass(SortKeyComparator.class);

            job2.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}
