import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TestHBaseReducer extends Reducer<Text, Text,Text,Text> {
    private String[] destinations;

    long counter = 0;

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String[] line = val.toString().split(",");
            if(key.toString().equals("#Date:Heure")){
                destinations= new String[line.length];
                for(int i = 0;i<line.length;i++)
                    destinations[i] = line[i];
                String s = "";
                for(int i=0; i<line.length ; i++){
                    s+=","+destinations[i];
                }
                context.write(new Text(key.toString()), new Text(s));
            }
            else{
                String s = "day="+key.toString().split(":")[0]+",hour="+key.toString().split(":")[1];
//                s+= "destinations="+",line="+line.length;
                for(int i=0; i<line.length ; i++){
                    s+=","+destinations[i]+"="+line[i];
                }
                context.write(new Text(key.toString()), new Text(s));

            }
//            context.write(key,new Text(key.toString().equals("Date:Heure")+""+(counter++)));
        }

    }
    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Analising Data");
        job.setJarByClass(TestReducer.class);
//        job.setMapperClass(TextToTextMapper.class);
//        job.setSortComparatorClass(null);
        job.setNumReduceTasks(1);
        job.setReducerClass(TestHBaseReducer.class);



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("testHour/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
