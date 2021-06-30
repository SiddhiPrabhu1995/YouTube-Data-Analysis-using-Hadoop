package com.neu.bigdata;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BinningByCategories {

    public static class YouTubeBinMapper extends Mapper<Object, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> mos = null;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        @Override
        protected void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {

            String[] input = value.toString().split(",");
            if (input.length > 2) {
            Text Name = new Text(input[3]);
            String line = Name.toString();
            if (line.contains("UNA ")) {
                mos.write("bins", value, NullWritable.get(), "UNA");
            } else if (line.contains("Autos & Vehicles")) {
                mos.write("bins", value, NullWritable.get(), "Autos & Vehicles");
            } else if (line.contains("Comedy")) {
                mos.write("bins", value, NullWritable.get(), "Comedy");
            } else if (line.contains("Entertainment")) {
                mos.write("bins", value, NullWritable.get(), "Entertainment");
            } else if (line.contains("Film & Animation")) {
                mos.write("bins", value, NullWritable.get(), "Film & Animation");
            } else if (line.contains("Gadgets & Games")) {
                mos.write("bins", value, NullWritable.get(), "Gadgets & Games");
            } else if (line.contains("Howto & DIY")) {
                mos.write("bins", value, NullWritable.get(), "Howto & DIY");
            } else if (line.contains("Music")) {
                mos.write("bins", value, NullWritable.get(), "Music");
            } else if (line.contains("News & Politics")) {
                mos.write("bins", value, NullWritable.get(), "News & Politics");
            } else if (line.contains("People & Blogs")) {
                mos.write("bins", value, NullWritable.get(), "People & Blogs");
            } else if (line.contains("Pets & Animals")) {
                mos.write("bins", value, NullWritable.get(), "Pets & Animals");
            } else if (line.contains("Sports")) {
                mos.write("bins", value, NullWritable.get(), "Sports");
            } else if (line.contains("Travel & Places")) {
                mos.write("bins", value, NullWritable.get(), "Travel & Places");
            } else {
                mos.write("bins", value, NullWritable.get(), "UnCatogrized");
            }
        }
        }
        @Override
        protected void cleanup(Mapper.Context context)
                throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Binning");
        job.setJarByClass(BinningByCategories.class);
        job.setMapperClass(YouTubeBinMapper.class);
        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
                Text.class, NullWritable.class);

        MultipleOutputs.setCountersEnabled(job, true);

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }

}

