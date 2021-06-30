package com.neu.bigdata;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewFollower extends Configured implements Tool {

    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "NewFollowers");
        job.setJarByClass(NewFollower.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new NewFollower(), args);

        System.exit(res);
    }

}

class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text user = new Text();
    private Text user1 = new Text();
    private Text user2 = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        int i, j;
        String s[] = value.toString().split(",");
        String val[];
        if (s.length > 1) {
            val = s[1].split(",");
        } else {
            val = null;
        }

        user.set(s[0]);
        if (val != null) {
            for (i = 0; i < val.length - 1; i++) {                user2.set(val[i] + "#-3000");
                context.write(user, user2);
                for (j = i + 1; j < val.length; j++) {
                    user1.set(val[i]);
                    user2.set(val[j] + "#1");
                    context.write(user1, user2);
                    user1.set(val[j]);
                    user2.set(val[i] + "#1");
                    context.write(user1, user2);

                }
            }
            user2.set(val[i] + "#-3000");
            context.write(user, user2);

        }

    }
}

class Reduce extends Reducer<Text, Text, IntWritable, Text> {

    class ValueComparator implements Comparator<String> {

        HashMap<String, Integer> base;

        public ValueComparator(HashMap<String, Integer> base) {
            this.base = base;
        }

        // Note: this comparator imposes orderings that are inconsistent with equals.    
        public int compare(String a, String b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        HashMap<String, Integer> map1 = new HashMap<String, Integer>();
        ValueComparator cmp = new ValueComparator(map1);
        TreeMap<String, Integer> map2 = new TreeMap<String, Integer>(cmp);

        while (values.iterator().hasNext()) {
            String s[] = values.iterator().next().toString().split("#");
            if (!map1.containsKey(s[0])) {
                map1.put(s[0], 0);
            }
            map1.put(s[0], map1.get(s[0]) + Integer.parseInt(s[1]));

        }
        map2.putAll(map1);

        ArrayList<String> list = new ArrayList<String>();

        int i = 0;

        for (Entry<String, Integer> e : map2.entrySet()) {
            if (i < 10 && e.getValue() > 0) {
                list.add(e.getKey());
            }
            ;
            i++;
        }

        String out = list.toString();
        context.write(new IntWritable(Integer.parseInt(key.toString())), new Text(out));
    }
}

