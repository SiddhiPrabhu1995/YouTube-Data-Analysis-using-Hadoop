package com.neu.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerClass extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
        // Initialize our result
        result.setAverageRating(0);
        result.setTotalRating(0);
        result.setTotalComment(0);
        int sum = 0;

        for (MinMaxCountTuple val : values) {
        	//max
            if (result.getAverageRating()== 0 || val.getAverageRating() < result.getAverageRating()) {
                result.setAverageRating(val.getAverageRating());
            }
            //min
            if (result.getTotalRating()== 0
                    || val.getTotalRating() > (result.getTotalRating())) {
                result.setTotalRating(val.getTotalRating());
            }
            //sum
            sum += val.getTotalComment();

        }
        result.setTotalComment(sum);
        context.write(key, result);
    }

}


