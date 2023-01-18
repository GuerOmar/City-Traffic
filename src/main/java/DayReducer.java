import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class DayReducer extends Reducer<LongWritable, Sensor,Text,Text> {
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

    public void reduce(LongWritable key, Iterable<Sensor> values,Context context) throws IOException, InterruptedException {
        for (Sensor sensor: values) {
            counter ++;
            String ch = sensor.date ;
            int[] counters = map.get(ch);
            if (counters == null){
                counters = new int[2];
                map.put(ch, counters);
            }

            if(sens[0].equals(""))
                sens[0] = sensor.direction;
            if(!sensor.direction.equals(sens[0]) && sens[1].equals(""))
                sens[1] = sensor.direction;

            if (sensor.direction.equals(sens[0])){
                counters[0]++;
            } else if (sensor.direction.equals(sens[1])){
                counters[1]++;
            }
        }

    }
}