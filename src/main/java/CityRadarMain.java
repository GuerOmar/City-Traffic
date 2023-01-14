import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CityRadarMain {
    public static class CityRadarMapper extends Mapper<LongWritable, Text, LongWritable, Radar> {
        int heure, minute, seconde, centieme;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            if(key.get() == 0)
                return;
            if (tokens.length < 7) return;
            if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
                    || tokens[4].length() ==0 || tokens[5].length() ==0 || tokens[6].length() ==0 )
                return ;
            if (tokens[2].length() < 4 || tokens[3].length() < 4)
                return;
            if (tokens[2].length() != 0){
                switch(tokens[2].length()) {
                    case 3:
                        heure = Integer.parseInt(tokens[2].substring(0, 1));
                        minute = Integer.parseInt(tokens[2].substring(1, 3));
                        break;
                    case 4:
                        heure = Integer.parseInt(tokens[2].substring(0, 2));
                        minute = Integer.parseInt(tokens[2].substring(2, 4));
                        break;
                    case 2:
                        heure = Integer.parseInt(tokens[2].substring(0, 1));
                        minute = Integer.parseInt(tokens[2].substring(1, 2));
                        break;
                    default:
                        minute = Integer.parseInt(tokens[2].substring(0, 1));
                        break;
                }
                }
            if (tokens[3].length() != 0){
                switch(tokens[3].length()) {
                    case 3:
                        seconde = Integer.parseInt(tokens[2].substring(0, 1));
                        centieme = Integer.parseInt(tokens[2].substring(1, 3));
                        break;
                    case 4:
                        seconde = Integer.parseInt(tokens[2].substring(0, 2));
                        centieme = Integer.parseInt(tokens[2].substring(2, 4));
                        break;
                    case 2:
                        seconde = Integer.parseInt(tokens[2].substring(0, 1));
                        centieme = Integer.parseInt(tokens[2].substring(1, 2));
                        break;
                    default:
                        seconde = Integer.parseInt(tokens[2].substring(0, 1));
                        break;
                }

            }
            context.write(key, new Radar(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]), heure, minute,seconde,centieme,tokens[4],tokens[5],tokens[6]));

        }
    }

    public static void main(String[] args) throws Exception {
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Cam Data");
        job.setJarByClass(CityRadarMain.class);
        job.setMapperClass(CityRadarMapper.class);
//        job.setReducerClass(CityTrafficReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Radar.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Radar.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytraffic/Data_cam_example.csv
        // testCamCleaning
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
