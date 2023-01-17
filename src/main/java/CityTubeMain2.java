import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CityTubeMain2 {

    public static class CityTubeMapper2 extends Mapper<LongWritable, Text, LongWritable, Tube> {
        public String[] directions ;
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            String direction = "";
            if(key.get() ==0)
                return;
            if (tokens.length < 6) return;
            if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
                    || tokens[4].length() ==0 || tokens[5].length() ==0  )
                return ;
            
            InputSplit inputSplit = context.getInputSplit();
            Path path = ((FileSplit)inputSplit).getPath();
            String fileName = path.getName();
            String[] tokens2 = fileName.split("_");
            direction = tokens2[1]+" "+tokens2[2];
            /* Pattern pattern = Pattern.compile("^(.*?)_(.*?)_(.*?)_");
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.find()) {
                direction  = matcher.group(2);
            } else {
                System.out.println("No match found");
            } */

            int heure = Integer.parseInt(tokens[1].split(":")[0]);
            int minute = Integer.parseInt(tokens[1].split(":")[1]) ;
            context.write(key,new Tube (direction,tokens[0],heure,minute,Integer.parseInt(tokens[2]),Integer.parseInt(tokens[3]),
                    Integer.parseInt(tokens[4]),tokens[5]));

        }
    }

    public static class CityTubeReducer extends Reducer<LongWritable, Tube,Text,Text> {
        private Map<String,int[]> map = new HashMap<>();

        private String[] sens = {"",""};
        long counter = 0;


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Date:Heure"), new Text("Nombres de voitures "+sens[0]+","+"Nombres de voitures "+sens[1]));
            for (Map.Entry<String, int[]> entry : map.entrySet()) {
                String mapKey = entry.getKey();
                int[] value = entry.getValue();
                context.write(new Text(mapKey),new Text(value[0]+","+value[1]));
            }

       }

        public void reduce(LongWritable key, Iterable<Tube> values,Context context) throws IOException, InterruptedException {
            for (Tube tube: values) {
                counter ++;
                String ch = tube.date +":"+ tube.heure ;
                int[] counters = map.get(ch);
                if (counters == null){
                    counters = new int[2];
                    map.put(ch, counters);
                }

                if(sens[0].equals(""))
                    sens[0] = tube.voie;
                if(!tube.voie.equals(sens[0]) && sens[1].equals(""))
                    sens[1] = tube.voie;

                if (tube.voie.equals(sens[0])){
                    counters[0]++;
                } else if (tube.voie.equals(sens[1])){
                    counters[1]++;
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // Multiple Input Format
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning Tube Data");
        job.setJarByClass(CityTubeMain2.class);
        job.setMapperClass(CityTubeMain2.CityTubeMapper2.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(CityTubeReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Tube.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // /user/auber/data_ple/citytube/Data_cam_example.csv
        // testCamCleaning
        for(int i = 0 ; i<args.length-1; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
