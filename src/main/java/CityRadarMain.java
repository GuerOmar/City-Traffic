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
            context.write(key, new Radar(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]), heure, minute,seconde,centieme,tokens[4],tokens[5],tokens[6]));

        }
    }

    public static class CityRadarReducer   extends Reducer<LongWritable, Radar, Text, Text> {
        private Map<String,int[]> map = new HashMap<>();
        long counter = 0;


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, int[]> entry : map.entrySet()) {
                String mapKey = entry.getKey();
                int[] value = entry.getValue();
                context.write(new Text(mapKey),new Text(value[0]+","+value[1]));
            }

//            context.write(new Text("Counter is "), new Text(""+counter));
//            context.write(new Text("HashMap length is") , new Text(""+ map.size()));
        }

        public void reduce(LongWritable key, Iterable<Radar> values,Context context) throws IOException, InterruptedException {
            for (Radar radar: values) {
                counter ++;
                String ch = radar.JOUR +":"+ radar.HEURE ;
                int[] counters = map.get(ch);
                if (counters == null){
                    counters = new int[2];
                    map.put(ch, counters);
                }
                if (radar.SENS == 1) {
                    counters[0]++;
                } else if (radar.SENS == 2) {
                    counters[1]++;
                }
            }



        }
    }

    public static void main(String[] args) throws Exception {
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Radar Data");

        job.setNumReduceTasks(1);

        job.setJarByClass(CityRadarMain.class);
        job.setMapperClass(CityRadarMapper.class);
        job.setReducerClass(CityRadarReducer.class);

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
