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

    public static class CityTrafficMapper extends Mapper<LongWritable, Text, LongWritable, Cam> {
        public String[] directions ;
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String tokens[] = value.toString().split(";");
//            if(key.get() == 0)
//                context.write(key,value);
            if (key.get() == 0){
                directions = new String[]{tokens[4], tokens[5]} ;
                return;
//                context.write(key, new Text(directions[0]+ ","+ directions[1]));
            }
            if(tokens.length <= 5) return;
            if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
            || (tokens[4].length() !=0 && tokens[5].length() !=0) || (tokens[4].length() ==0 && tokens[5].length() ==0))
                return ;

            String direction = "";
            if(tokens[4].length() != 0 )
                direction = directions[0];
            if(tokens[5].length() != 0)
                direction = directions[1];
            context.write(key,new Cam (Integer.parseInt(tokens[0]),tokens[1],tokens[2],tokens[3], false,direction));

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
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Cam Data");
        job.setJarByClass(CityTrafficMain.class);
        job.setMapperClass(CityTrafficMapper.class);
//        job.setReducerClass(CityTrafficReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Cam.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Cam.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytraffic/Data_cam_example.csv
        // testCamCleaning
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
