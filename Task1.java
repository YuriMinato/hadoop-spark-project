import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Task1 {

    // add code here
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("[,|\n]");
            word.set(tokens[0]);
            int max = 0;
            Text index = new Text();
            for (int i = 1; i < tokens.length; i++) {

                if (!tokens[i].equals("")) {
                    //&& Pattern.compile("[0-9]*").matcher(tokens[i]).matches()
                    if (tokens[i].equals("5")) {
                        max = 5;
                        break;
                    } else if (Integer.parseInt(tokens[i]) > max) {
                        max = Integer.parseInt(tokens[i]);
                    }

                }

            }
            StringBuffer sb = new StringBuffer();
            for (int i = 1; i < tokens.length; i++) {
                if (!tokens[i].equals("") && Integer.parseInt(tokens[i]) == max) {

//                    String s = i + ",";
//                    index.append(s.getBytes(), 0, s.getBytes().length);
                    sb.append(i + ",");


                }

            }
            sb.deleteCharAt(sb.length() - 1);
            index.set(sb.toString());
            context.write(word, index);
        }
    }
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
