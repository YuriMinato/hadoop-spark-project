import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {
  public static class TokenizerMapper
          extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private IntWritable num = new IntWritable();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\n");
      String[] tmp = tokens[0].split("," ,-1);

      for (int i = 1; i < tmp.length; i++) {
        num.set(i);
        if (tmp[i].equals("")) {
          context.write(num, zero);
        } else {
          context.write(num, one);
        }
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: Task3 <in> <out>");
      System.exit(2);
    }

    job.setJarByClass(Task3.class);

    job.setMapperClass(Task3.TokenizerMapper.class);
    job.setCombinerClass(Task3.IntSumReducer.class);
    job.setReducerClass(Task3.IntSumReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
