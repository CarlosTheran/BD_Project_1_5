package Part5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;
/**
 * Created by carlos on 03-25-17.
 */
public class TwitterRepliesMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long Reply_ID = status.getId();
            long ReplyTo_ID = status.getInReplyToStatusId();    //getInReplyToUserId();

            //ReplyTo_ID = (int) ReplyTo_ID;
            //long originaluserid = originalTweet.getUser().getId();


            if(ReplyTo_ID != -1)
            {
              context.write(new Text(Long.toString(ReplyTo_ID)), new Text(Long.toString(Reply_ID)));
            }
            //)

        }
        catch(TwitterException e){

        }

    }

}
