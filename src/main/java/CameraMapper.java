import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

public class CameraMapper extends Mapper<LongWritable, Text, LongWritable, Sensor> {
    public String[] directions = new String[2];
    int heure, minute, seconde, centieme;
    String date ="";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String tokens[] = value.toString().split(",");
        if (key.get() == 0 && tokens.length == 5){
            directions[0] = tokens[3];
            directions[1] = tokens[4];
            return;
        }
        if(tokens.length <4) return;
        if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0
            || (tokens.length == 5 && tokens[3].length()!=0 && tokens[4].length()==0)
            || (tokens.length == 4 && tokens[3].length()==0))
            return ;

        String direction = "";
        if(tokens.length == 4 )
            direction = directions[0];
        if(tokens.length == 5)
            direction = directions[1];

        InputSplit inputSplit = context.getInputSplit();
        Class<? extends InputSplit> splitClass = inputSplit.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) inputSplit;
        } else if (splitClass.getName().equals(
                "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            // begin reflection hackery...

            try {
                Method getInputSplitMethod = splitClass
                        .getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(inputSplit);
            } catch (Exception e) {
                // wrap and re-throw error
                throw new IOException(e);
            }

            // end reflection hackery
        }
        Path path = fileSplit .getPath();
        String fileName = path.getName();
        String[] tokens2 = fileName.split("\\.");

        String[] horodate = tokens[2].split(" ");


        date = horodate[0].split("/")[2]+"/"+horodate[0].split("/")[1]+"/"+horodate[0].split("/")[0];

        String[] heureMinute = horodate[1].split(":");
        heure = Integer.parseInt(heureMinute[0]);
        minute = Integer.parseInt(heureMinute[1]);

        String[] secondeCentieme = heureMinute[2].split("\\.");
        seconde = Integer.parseInt(secondeCentieme[0]);
        centieme = Integer.parseInt(secondeCentieme[1]);

        
       

        context.write(key,new Sensor (tokens2[0],direction, date,heure,minute,seconde,centieme,0,"" ));
    }
}
