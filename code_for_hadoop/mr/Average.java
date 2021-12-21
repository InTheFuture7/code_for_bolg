package mr.demo1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Average {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJarByClass(Average.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 设置data文件夹权限为777，以便可创建output文件夹
		Path in = new Path("file:///usr/local/java/data/mapreduce_demo/input/data_click"); // 用本地文件输入
		Path out = new Path("file:///usr/local/java/data/mapreduce_demo/output"); // 结果输出到本地，文件夹不能已经存在

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
			StringTokenizer tokenizerLine = new StringTokenizer(tokenizer.nextToken()); // 默认分隔符为空格
			String strId = tokenizerLine.nextToken();
			String strScore = tokenizerLine.nextToken();
			Text id = new Text(strId);
			int score = Integer.parseInt(strScore);
			context.write(id, new IntWritable(score));
		}
	}


	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				int score = iterator.next().get();
				System.out.println(score);
				sum += score;
				count++;
			}
			int avg = sum / count;
			context.write(key, new IntWritable(avg));
		}
	}
}
