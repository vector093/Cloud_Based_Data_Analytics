package line.count;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineCount{
    public static class LineCntMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text keyone = new Text("Total Lines");
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context){
        //read the line and set a constant key(total lines) and set a constant value 1 for every line
    	try {
            context.write(keyone, one);
        } 
        catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } 
        catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}

    public static class LineCntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        int sum = 0;
        // as all the input pair have the same key we just sum all the values and set the same key as output key and the summed value as the output value
        for (IntWritable val : values) {
            sum += val.get();
        }
        try {
            context.write(key, new IntWritable(sum));
        } 
        catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } 
        catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "line count2");
    job.setJarByClass(LineCount.class);
    job.setMapperClass(LineCntMapper.class);
    job.setCombinerClass(LineCntReducer.class);
    job.setReducerClass(LineCntReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
