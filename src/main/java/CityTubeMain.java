import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CityTubeMain {

    public static class CityTubeMapper extends Mapper<LongWritable, Text, LongWritable, Tube> {
        public String[] directions ;
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            if(key.get() ==0)
            return;
            if (tokens.length < 9) return;
            if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
            || tokens[4].length() ==0 || tokens[5].length() ==0 || tokens[8].length() ==0 )
            return ;
            int heure = Integer.parseInt(tokens[2].split(":")[0]);
            int minute = Integer.parseInt(tokens[2].split(":")[1]) ;
            context.write(key,new Tube (Integer.parseInt(tokens[0]),tokens[1],heure,minute,Integer.parseInt(tokens[3]),Integer.parseInt(tokens[4]),
                    Integer.parseInt(tokens[5]),tokens[8]));

        }
    }

    public static class CityTubeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Cam Data");
        job.setJarByClass(CityTubeMain.class);
        job.setMapperClass(CityTubeMapper.class);
//        job.setReducerClass(CityTubeReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Tube.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Tube.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytube/Data_cam_example.csv
        // testCamCleaning
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
