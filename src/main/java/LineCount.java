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

public class LineCount {

    private static Logger logger = Logger.getLogger(LineCount.class);

    public static class LineCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private Logger logger = Logger.getLogger(LineCountMapper.class);

        //For the latest information about Hadoop, please visit our website at:


        //1, For the latest information about Hadoop, please visit our website at:
        //2, http://hadoop.apache.org/core/
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //int => IntWritable
            String line = value.toString();
            logger.info("line "+ line);
            if (line != null && !line.isEmpty())
                context.write(new Text("No of Lines"), new IntWritable(1));

            //Shuffle and sort

        }
    }

    public static class LineCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
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
        logger.info("hello");
        Job job = Job.getInstance(conf, "line count");
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