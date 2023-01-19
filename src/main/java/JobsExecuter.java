import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.ToolRunner;

public class JobsExecuter {
    public static void main(String[] args) throws Exception {
        String[] multieArgs = {args[0], "city_traffic_data"};
        int exitCode =ToolRunner.run(new MultiMapperDriver(), multieArgs);
        String[] testArgs = {"/user/oguermazi/city_traffic_data/part-r-00000","hour_reducer_data"};
        int exitCode2 =ToolRunner.run(new Test(), testArgs);
//        ProgramDriver pgd = new ProgramDriver();
//        int exitCode = -1;
//        try {
//            pgd.addClass("Clean", MultiMapperDriver.class, "Clean Data");
//            pgd.addClass("HourReducer", Test.class, "analise data");
//            pgd.
//            exitCode = pgd.run(args);
//        } catch (Throwable e1)  {
//            e1.printStackTrace();
//        }
        System.exit(exitCode);
    }
}
