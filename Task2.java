import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Task2 {

    // add code here
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

        private long count = 0;

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            // "[,|\n]"

            for (int i = 1; i < tokens.length; i++) {
                if (!tokens[i].equals(""))
                    count++;

            }
            //context.write(new LongWritable(count), NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(count), NullWritable.get());
        }

    }

    public static class IntSumReducer
            extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

        private long count = 0;

        public void reduce(LongWritable key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            count += key.get();


            //context.write(new LongWritable(count), NullWritable.get());

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(count), NullWritable.get());
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
