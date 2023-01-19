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
        String[] testArgs = {"city_traffic_data/part-r-00000","hour_reducer_data"};
        int exitCode2 =ToolRunner.run(new ReducerAdaptaterDriver(HourReducer.class), testArgs);
        testArgs[1]="day_reducer_data";
        int exitCode3 =ToolRunner.run(new ReducerAdaptaterDriver(DayReducer.class), testArgs);
        testArgs[1]="vehicule_reducer_data";
        int exitCode4 =ToolRunner.run(new ReducerAdaptaterDriver(VehicleReducer.class), testArgs);
        testArgs[1]="sensor_reducer_data";
        int exitCode5 =ToolRunner.run(new ReducerAdaptaterDriver(SensorReducer.class), testArgs);
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
