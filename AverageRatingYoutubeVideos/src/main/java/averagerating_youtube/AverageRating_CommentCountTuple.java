/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagerating_youtube;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class AverageRating_CommentCountTuple implements Writable {

    private int comment_count = 0;
    private double video_rating = 0;

    public int getComment_count() {
        return comment_count;
    }

    public void setComment_count(int comment_count) {
        this.comment_count = comment_count;
    }

    public double getVideo_rating() {
        return video_rating;
    }

    public void setVideo_rating(double video_rating) {
        this.video_rating = video_rating;
    }

    
    public void write(DataOutput d) throws IOException {
        d.writeInt(comment_count);
        d.writeDouble(video_rating);

    }

    
    public void readFields(DataInput di) throws IOException {

        comment_count = di.readInt();
        video_rating = di.readDouble();

    }

    @Override
    public String toString() {
        return Integer.toString(comment_count) + " " + Double.toString(video_rating);
    }

}
