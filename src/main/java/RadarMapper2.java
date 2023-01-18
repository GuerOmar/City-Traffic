import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;

public class RadarMapper2 extends Mapper<LongWritable, Text, LongWritable, Sensor> {
    int heure, minute, seconde, centieme;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String tokens[] = value.toString().split(",");
        if(key.get() == 0)
            return;
        if (tokens.length < 5) return;
        if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
                || tokens[4].length() ==0 )
            return ;
        String[] horodate = tokens[0].split(" ");
        
        String date  =horodate[0];
 
        String[] heureMinute = horodate[1].split(":");
        heure = Integer.parseInt(heureMinute[0]);
        minute = Integer.parseInt(heureMinute[1]);

        String[] secondeCentieme = heureMinute[2].split("\\.");
        seconde = Integer.parseInt(secondeCentieme[0]);
        centieme = Integer.parseInt(secondeCentieme[1]); 

        InputSplit inputSplit = context.getInputSplit();
        Path path = ((FileSplit)inputSplit).getPath();
        String fileName = path.getName();
        String[] tokens2 = fileName.split("\\.");

          context.write(key, new Sensor(tokens2[0],tokens[1], date, heure, minute, seconde, centieme, Double.parseDouble(tokens[2]), tokens[4])); 

    }
}
