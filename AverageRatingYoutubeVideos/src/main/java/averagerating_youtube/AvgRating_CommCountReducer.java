package averagerating_youtube;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgRating_CommCountReducer extends Reducer<Text, AverageRating_CommentCountTuple, Text, AverageRating_CommentCountTuple> {

    private AverageRating_CommentCountTuple result = new AverageRating_CommentCountTuple();

    protected void reduce(Text key, Iterable<AverageRating_CommentCountTuple> values, Context context) throws IOException, InterruptedException {

        float sum = 0;
        int count = 0;

        for (AverageRating_CommentCountTuple val : values) {
            sum += val.getComment_count() * val.getVideo_rating();
            count += val.getComment_count();
        }

        result.setVideo_rating(sum / count);
        //result.setComment_count(count);
        context.write(key, result);

    }
}
