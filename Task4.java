import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        private Text result = new Text();
        private static Map<String, List<Integer>> nameList = new HashMap<>();

        public void setup(Context context) throws IOException,
                InterruptedException {
            try {
                Path filePath = context.getLocalCacheFiles()[0];
                BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
                String line;
                boolean tmp;
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split("," , -1);
                    String name = tokens[0];
                    List<Integer> scores = new ArrayList<>();
                    for (int i = 1; i < tokens.length; i++) {
                        if (tokens[i].equals("")) {
                            tmp = scores.add(0);
                        } else {
                            tmp = scores.add(Integer.parseInt(tokens[i]));
                        }
                    }
                    nameList.put(name, scores);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String nameLine = value.toString();
            String name = "";
            for (int i = 0; i < nameLine.length(); i++) {
                if (nameLine.charAt(i) == ',') {
                    name = nameLine.substring(0, i);
                    break;
                }
            }

            List<Integer> scores = nameList.get(name);
            for (String anotherName : nameList.keySet()) {
                if (name.compareTo(anotherName) < 0) {
                    List<Integer> anotherScores = nameList.get(anotherName);
                    int sum = 0;
                    for (int i = 0; i < scores.size(); i++) {
                        if (scores.get(i) != 0 && scores.get(i) == anotherScores.get(i)) {
                            sum++;
                        }
                    }
                    StringBuilder sb = new StringBuilder();
                    String info = sb.append(name).append(',').append(anotherName).append(',').append(sum).toString();
                    result.set(info);
                    context.write(result, NullWritable.get());
                }
            }
        }
            }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task 4");
        job.setJarByClass(Task4.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        job.setMapperClass(Task4.TokenizerMapper.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(new URI(otherArgs[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
