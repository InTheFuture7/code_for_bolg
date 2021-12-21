package mr.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DoMapper extends Mapper<Object, Text, Text, IntWritable> {
    public static final IntWritable one = new IntWritable(1);
    public static Text word = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // StringTokenizer-java
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\t");
        word.set(tokenizer.nextToken());
        context.write(word, one);

    }
}
