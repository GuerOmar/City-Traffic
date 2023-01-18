import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CityRadarMain2 {
    public static class CityRadarMapper2 extends Mapper<LongWritable, Text, LongWritable, Radar> {
            int heure, minute, seconde, centieme, jour;
            String direction = "";
            @Override
            protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
                String tokens[] = value.toString().split(",");
                if(key.get() == 0)
                    return;
                if (tokens.length < 5) return;
                if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
                        || tokens[4].length() ==0 )
                    return ;
                String[] horodate = tokens[0].split(" ");
                
                jour = Integer.parseInt(horodate[0].split("/")[0]);
         
                String[] heureMinute = horodate[1].split(":");
                heure = Integer.parseInt(heureMinute[0]);
                minute = Integer.parseInt(heureMinute[1]);
        
                String[] secondeCentieme = heureMinute[2].split("\\.");
                seconde = Integer.parseInt(secondeCentieme[0]);
                centieme = Integer.parseInt(secondeCentieme[1]); 
        
                  context.write(key, new Radar(tokens[1],jour,
                 heure, minute,seconde,centieme,Double.parseDouble(tokens[2]),
                 0,tokens[4])); 
        
            
            
        }
    }


    public static void main(String[] args) throws Exception {
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Radar Data");

        job.setNumReduceTasks(1);

        job.setJarByClass(CityRadarMain2.class);
        job.setMapperClass(CityRadarMapper2.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Radar.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytraffic/Data_cam_example.csv
        // testCamCleaning
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
