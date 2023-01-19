import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HourReducer extends Reducer<LongWritable, Sensor,Text,Text> {
    private Map<String,int[]> map = new HashMap<>();

    private String[] sens = new String[50];
    long counter = 0;


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String text ="";
        int j = 0;
        while(sens[j] !=null){
            text+=","+"Nombres de voitures "+sens[j];
            j++;
        }
        text = text.substring(1);
        context.write(new Text("Date:Heure"), new Text(text));
        for (Map.Entry<String, int[]> entry : map.entrySet()) {
            String mapKey = entry.getKey();
            int[] value = entry.getValue();
            j = 0;
            String valuesText = "";
            while(sens[j] !=null){
                valuesText+=","+value[j];
                j++;
            }
            valuesText = valuesText.substring(1);
            context.write(new Text(mapKey),new Text(valuesText));
        }

   }

    public void reduce(LongWritable key, Iterable<Sensor> values,Context context) throws IOException, InterruptedException {
        for (Sensor sensor: values) {
            counter ++;
            String ch = sensor.date+ ":"+sensor.heure ;
            int[] counters = map.get(ch);
            if (counters == null){
                counters = new int[50];
                map.put(ch, counters);
            }
            int i = 0 ;
            while(sens[i] !=null && !sensor.direction.equals(sens[i]))
                i++;
            if(sens[i] == null){
                sens[i] = sensor.direction;
            }


            counters[i]++;

        }

    }
}
