package mr.demo1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 将 HDFS 上的文本作为输入，MapReduce 通过 InputFormat 会将文本进行切片处理，
 * 并将每行的首字母相对于文本文件的首地址的偏移量作为输入键值对的 key，文本内容作为输入键值对的 value，
 * 经过在 map 函数处理，输出中间结果<word，1>的形式，并在 reduce 函数中完成对每个单词的词频统计
 * 整个程序代码主要包括两部分：Mapper 部分和 Reducer 部分
 * 
 * wordcount程序会往复执行直到 job.waitForCompletion(true)
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 启动job任务
        Job job = Job.getInstance();
        job.setJobName("WordCount");
        // 设置mapper类、Reducer类
        job.setJarByClass(WordCount.class);
        job.setMapperClass(doMapper.class);
        job.setReducerClass(doReducer.class);

        // 设置Job输出结果<key,value>的中key和value数据类型，因为结果是<单词,个数>，所以key设置为"Text"类型，Value设置为"IntWritable"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path in = new Path("hdfs://localhost:9000/mr_demo/input/cust_fav"); //需要统计的文本所在位置
        Path out = new Path("hdfs://localhost:9000/mr_demo/output");  // 输出文件夹不能存在
        // job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        // System.exit(0）退出程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



	 /**
     * @param KEYIN
     *            →k1 表示每一行的起始位置（偏移量offset）
     * @param VALUEIN
     *            →v1 表示每一行的文本内容
     * @param KEYOUT
     *            →k2 表示每一行中的每个单词
     * @param VALUEOUT
     *            →v2 表示每一行中的每个单词的出现次数，固定值为1
     */
    public static class doMapper extends Mapper<Object, Text, Text, IntWritable> {
        /*
        读取每一行文本数据，构成 偏移量-文本 的键值对，如： 1 word1,word2,word3 /n 2 word2,word3
        对 文本 按特定分隔符拆分为一个个 字符串（单词）
        返回 字符串（单词）-1 形式的键值对，如：word1 1 /n word2 1 /n word3 1 /n word2 1 /n word3 1

        参数：
            KEYIN：行偏移量，数字
            VALUEIN：v1类型，字符串-Test（hadoop.io）
            KEYOUT：k2类型，字符串-Test（hadoop.io）
            VALUEOUT：v1类型，数字-IntWritable
        */
        public static final IntWritable one = new IntWritable(1);
        public static Text word = new Text();

        // 重写map方法，将k1,v1转为k2，v2
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
            key：KEYIN
            value：VALUEIN
            context：上下文对象
            */
            // 拆分文本数据，返回当前位置到下一个分隔符之间的字符串，以“制表符”为分隔符来分隔字符串
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\t");
            // 将分割后的字符串放入set中
            word.set(tokenizer.nextToken());
            // 将 word-1 存到容器中
            context.write(word, one);
        }
    }

	
    /**
     * @param KEYIN
     *            →k2 表示每一行中的每个单词
     * @param VALUEIN
     *            →v2 表示每一行中的每个单词的出现次数，固定值为1
     * @param KEYOUT
     *            →k3 表示每一行中的每个单词
     * @param VALUEOUT
     *            →v3 表示每一行中的每个单词的出现次数之和
     */
    public static class doReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        /*
        将map、shuffle操作后的结构 字符串（单词）-<1,1,…> 中的集合<1,1,…>依次遍历排序，如： word1 <1> /n word2 <1,1> /n word3 <1,1>
        返回为 字符串（单词）-次数，如：word1 1 /n word2 2 /n word3 2

        参数
            Text key：文本
            Iterable<IntWritable> values：集合
        */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // 遍历集合，将集合中的数字相加
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            System.out.println(sum);
            context.write(key, result);
        }
    }
}
