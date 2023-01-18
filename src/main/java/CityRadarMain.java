import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CityRadarMain {
    public static class CityRadarMapper extends Mapper<LongWritable, Text, LongWritable, Radar> {
        int heure, minute, seconde, centieme;
        String direction = "";
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
            if (tokens[2].length() > 4 || tokens[3].length() > 4)
                return;
            if (tokens[2].length() != 0){
                if (tokens[2].length() == 3) {
                    heure = Integer.parseInt(tokens[2].substring(0, 1));
                    minute = Integer.parseInt(tokens[2].substring(1, 3));
                } else if (tokens[2].length() == 4) {
                    heure = Integer.parseInt(tokens[2].substring(0, 2));
                    minute = Integer.parseInt(tokens[2].substring(2, 4));
                } else {
                    heure = 0;
                    minute = Integer.parseInt(tokens[2]);
                }
            }
            if (tokens[3].length() != 0){
                if (tokens[3].length() == 3) {
                    seconde = Integer.parseInt(tokens[3].substring(0, 1));
                    centieme = Integer.parseInt(tokens[3].substring(1, 3));
                } else if (tokens[3].length() == 4) {
                    seconde = Integer.parseInt(tokens[3].substring(0, 2));
                    centieme = Integer.parseInt(tokens[3].substring(2, 4));
                } else {
                    seconde = 0;
                    centieme = Integer.parseInt(tokens[3]);
                }
            }

            InputSplit inputSplit = context.getInputSplit();
            Path path = ((FileSplit)inputSplit).getPath();
            String fileName = path.getName();
            String[] tokens2 = fileName.split("_");
            direction = tokens2[1]+" "+tokens2[2];


            context.write(key, new Radar(direction,Integer.parseInt(tokens[1]),
             heure, minute,seconde,centieme,Double.parseDouble(tokens[4].split("=")[1]),
             Integer.parseInt(tokens[5].split("=")[1]),tokens[6]));

        }
    }



    public static void main(String[] args) throws Exception {
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Radar Data");

        job.setNumReduceTasks(1);

        job.setJarByClass(CityRadarMain.class);
        job.setMapperClass(CityRadarMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Radar.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytraffic/Data_cam_example.csv
        // testCamCleaning
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
