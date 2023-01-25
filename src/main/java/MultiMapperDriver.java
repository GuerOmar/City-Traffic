import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;

public class MultiMapperDriver extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaning All files");
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

            MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, mapperClass);

        }
        br.close();


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
