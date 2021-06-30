package com.neu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.FloatWritable;

public class Top_Youtuber {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top50");
        job.setJarByClass(Top_Youtuber.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TopNMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        private FloatWritable video_rating = new FloatWritable();
        private Text video_id = new Text();

        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            video_id = new Text(fields[0]);
            try {
            if(fields.length > 6) {
            //if (!fields[6].isEmpty()) {
                video_rating = new FloatWritable(Float.parseFloat(fields[7]));
            }

            context.write(video_id, video_rating);
            }catch (Exception e) {
            	e.printStackTrace();
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private Map<Text, FloatWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            float sum = 0.0f;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            countMap.put(new Text(key), new FloatWritable(sum / count));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, FloatWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 50) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

}

