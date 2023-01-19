import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TextToSensorMapper extends Mapper<LongWritable, Sensor, LongWritable, Sensor> {
    @Override
    protected void map(LongWritable key, Sensor value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
