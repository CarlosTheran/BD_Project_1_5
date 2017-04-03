package Part5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by carlos on 03-25-17.
 */
public class TwitterRepliesReducer extends Reducer<Text, Text, Text, Text>  {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String result = "[ ";
        int cont=0;

        for (Text value : values){
            result = result + value;
            result = result + " , ";
            cont =cont + 1;
        }

        Integer.toString(cont);
        result = result + cont;
        result = result + " ]";
        //concatenar
        context.write(key, new Text(result));

    }


}
