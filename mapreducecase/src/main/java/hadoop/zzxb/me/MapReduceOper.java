package hadoop.zzxb.me;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import java.util.StringTokenizer;

/**
 * Created by zzxb on 17/7/26.
 */
public class MapReduceOper extends Configured implements Tool{
    public static class ModelMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strValue = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(strValue);
            while (stringTokenizer.hasMoreTokens()){
                String str = stringTokenizer.nextToken();
                Text outKeyValue = new Text(str);
                context.write(outKeyValue,new IntWritable(1));
            }
        }
    }
    public static class ModelReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:
                 values) {
                sum = sum + value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    public int run(String[] args) throws Exception {
        //连接HDFS系统
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS","hdfs://hadoop.skedu.com:9000");

        Configuration configuration = getConf();

        //生成Job对象
        Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        //input-> map -> reduce -> output

        //设置input
        Path path = new Path(args[0]);
        FileInputFormat.addInputPath(job,path);

        //设置map
        job.setMapperClass(ModelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce
        job.setReducerClass(ModelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outPath);

        //提交任务
        boolean isSucc = job.waitForCompletion(true);

        return isSucc ? 0 : 1;
    }
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop.skedu.com:9000");
        int status = ToolRunner.run(configuration,new MapReduceOper(),args);
        System.exit(status);
    }
}
