package com.neu.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class MinMaxCountTuple implements Writable {

    private float averageRating;
    private float totalRating;
    private float totalComment;

    public float getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(float averageRating) {
        this.averageRating = averageRating;
    }

    public float getTotalRating() {
        return totalRating;
    }

    public void setTotalRating(float totalRating) {
        this.totalRating = totalRating;
    }    
    
    public float getTotalComment() {
        return totalComment;
    }

    public void setTotalComment(float totalComment) {
        this.totalComment = totalComment;
    }

    //@Override
    public void write(DataOutput d) throws IOException {
        d.writeFloat(averageRating);
        d.writeFloat(totalRating);
        d.writeFloat(totalComment);

    }

    //@Override
    public void readFields(DataInput di) throws IOException {
        averageRating = di.readFloat();
        totalRating = di.readFloat();
        totalComment = di.readFloat();

    }

    @Override
    public String toString() {
        return (averageRating + "\t" + totalRating + "\t" + totalComment);
    }

}

