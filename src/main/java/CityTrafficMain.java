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
import java.util.HashMap;
import java.util.Map;

public class CityTrafficMain {

    public static class CityTrafficMapper extends Mapper<LongWritable, Text, LongWritable, Cam> {
        public String[] directions = new String[2];
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            if (key.get() == 0 && tokens.length == 5){
                directions[0] = tokens[3];
                directions[1] = tokens[4];
                return;
            }
            if(tokens.length <4) return;
            if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0
                || (tokens.length == 5 && tokens[3].length()!=0 && tokens[4].length()==0)
                || (tokens.length == 4 && tokens[3].length()==0))
                return ;

            String direction = "";
            if(tokens.length == 4 )
                direction = directions[0];
            if(tokens.length == 5)
                direction = directions[1];

            context.write(key,new Cam (Integer.parseInt(tokens[0]),tokens[1],tokens[2].split(" ")[0],tokens[2].split(" ")[1],direction));

        }
    }

    public static class CityTrafficReducer extends Reducer<LongWritable,Cam,Text,Text> {
        private Map<String,int[]> map = new HashMap<>();
        private String[] directions = {"",""};
        long counter = 0;


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Date:Heure"), new Text("Nombres de voitures "+directions[0]+","+"Nombres de voitures "+directions[1]));
            for (Map.Entry<String, int[]> entry : map.entrySet()) {
                String mapKey = entry.getKey();
                int[] value = entry.getValue();
                context.write(new Text(mapKey),new Text(value[0]+","+value[1]));
            }

//            context.write(new Text("HashMap length is") , new Text(""+ map.size()));
        }

        public void reduce(LongWritable key, Iterable<Cam> values,Context context) throws IOException, InterruptedException {
            for (Cam cam: values) {
                counter ++;
                String ch = cam.date +":"+ cam.time.split(":")[0] ;
                int[] counters = map.get(ch);
                if (counters == null){
                    counters = new int[2];
                    map.put(ch, counters);
                }

                if(directions[0].equals(""))
                    directions[0] = cam.direction;
                if(!cam.direction.equals(directions[0]) && directions[1].equals(""))
                    directions[1] = cam.direction;

                if (cam.direction.equals(directions[0])) {
                    counters[0]++;
                } else if (cam.direction.equals(directions[1])) {
                    counters[1]++;
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Cam Data");
        job.setJarByClass(CityTrafficMain.class);
        job.setMapperClass(CityTrafficMapper.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(CityTrafficReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Cam.class);
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
