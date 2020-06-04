import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;

public class WordCount {

    private static Logger logger = Logger.getLogger(LineCount.class);

    public static class LineCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private Logger logger = Logger.getLogger(LineCountMapper.class);


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] words=line.split(",");
            for(String word: words )
            {
                Text outputKey = new Text(word.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(1);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class LineCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value : values)
            {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        logger.info("hello");
        Job job = Job.getInstance(conf, "line count");
        //job.setNumReduceTasks(2);
        job.setJarByClass(LineCount.class);
        job.setMapperClass(LineCountMapper.class);
        job.setCombinerClass(LineCountReducer.class);
        job.setReducerClass(LineCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}