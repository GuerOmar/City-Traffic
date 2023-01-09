import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CityTrafficMain {

    public static class CityTrafficMapper extends Mapper<LongWritable, Text, Text, LongWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String tokens[] = value.toString().split(";");
            if (tokens.length < 6) return;
            context.write(new Text(tokens[0].toString()),new LongWritable(new Integer(tokens[0])));

        }
    }

    public static class CityTrafficReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
//            int max = 0;
//            for (IntWritable val : values) {
//                if (val.get() > max) {
//                    max = val.get();
//                }
//            }
//            context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Cam Data");
        job.setJarByClass(CityTrafficMain.class);
        job.setMapperClass(CityTrafficMapper.class);
        job.setReducerClass(CityTrafficReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytraffic/Data_cam_example.csv
        // testCamCleaning
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
