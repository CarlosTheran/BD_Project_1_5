package Part5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


/**
 * Created by carlos on 03-25-17.
 */

public class TwitterKeyDriver {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        if (args.length != 2) {
            System.err.println("Usage: TwitterKeyWorkDriver <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Part5.TwitterKeyDriver.class);
        job.setJobName("Count Different_Word_by_Tweets");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Part5.TwitterRepliesMapper.class);
        job.setReducerClass(Part5.TwitterRepliesReducer.class);
        //job.setCombinerClass(edu.uprm.cse.bigdata.mrsp02.TwitterReduceByScreenName.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
