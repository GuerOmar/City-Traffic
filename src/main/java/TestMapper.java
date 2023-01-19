import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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



public class TestMapper extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Analising Data");
        job.setJarByClass(TestMapper.class);
        job.setMapperClass(RadarMapper2.class);

        job.setNumReduceTasks(0);
        //job.setReducerClass(SensorReducer.class);



        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Sensor.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Sensor.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return (job.waitForCompletion(true) ? 0 : 1);

         
    }
    public static void main(String args[]) throws Exception {
        System.exit(ToolRunner.run(new TestMapper(), args));
    }

}