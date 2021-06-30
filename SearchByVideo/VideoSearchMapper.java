/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package searchbyvideo;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;

class VideoSearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Funnel<Video> p = new Funnel<Video>() {
        @Override
        public void funnel(Video video, Sink into) {
            into.putString(video.videoID, Charsets.UTF_8);
        }
    };

    private BloomFilter<Video> video_id = BloomFilter.create(p, 500, 0.1);

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String v = conf.get("videoId");
        Video video = new Video(v);
        System.out.println(video);
        video_id.put(video);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] input = value.toString().split(",");
        Video video = new Video(input[0]);
        if (video_id.mightContain(video)) {
            context.write(value, NullWritable.get());

        }
    }

}
