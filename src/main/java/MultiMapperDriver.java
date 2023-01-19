import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MultiMapperDriver extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Multi Mapper Job");
        job.setJarByClass(MultiMapperDriver.class);

        // Reading File paths and corresponding Mapper Class Name
        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        String line = br.readLine(); // reading head
        while ((line = br.readLine()) != null) {
            // Parse the CSV line and add the mapping to the map
            String[] tokens = line.split(",");
            String path = tokens[0];
            String classname = tokens[1];
            Class mapperClass = Class.forName(classname);

            // Add the input path and mapper class for this file
            MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, mapperClass);

        }
        br.close();

//        // Set the input paths and mappers for each file type
//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CameraMapper.class);
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RadarMapper1.class);
//        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TubeMapper.class);

        job.setReducerClass(CombinerReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Sensor.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Sensor.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String args[]) throws Exception {
        System.exit(ToolRunner.run(new MultiMapperDriver(), args));
    }
}
