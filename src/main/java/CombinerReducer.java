import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class CombinerReducer extends Reducer<LongWritable, Sensor,LongWritable,Sensor> {




    public void reduce(LongWritable key, Iterable<Sensor> values,Context context) throws IOException, InterruptedException {
        for (Sensor sensor: values) {
            context.write(key,sensor);
        }

    }
}