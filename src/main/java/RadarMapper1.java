import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;
import java.lang.reflect.Method;

public class RadarMapper1 extends Mapper<LongWritable, Text, LongWritable, Sensor> {
    int heure, minute, seconde, centieme;
    String direction = "";
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String tokens[] = value.toString().split(",");
        if(key.get() == 0)
            return;
        if (tokens.length < 7) return;
        if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
                || tokens[4].length() ==0 || tokens[5].length() ==0 || tokens[6].length() ==0 )
            return ;
        if (tokens[2].length() > 4 || tokens[3].length() > 4)
            return;
        if (tokens[2].length() != 0){
            if (tokens[2].length() == 3) {
                heure = Integer.parseInt(tokens[2].substring(0, 1));
                minute = Integer.parseInt(tokens[2].substring(1, 3));
            } else if (tokens[2].length() == 4) {
                heure = Integer.parseInt(tokens[2].substring(0, 2));
                minute = Integer.parseInt(tokens[2].substring(2, 4));
            } else {
                heure = 0;
                minute = Integer.parseInt(tokens[2]);
            }
        }
        if (tokens[3].length() != 0){
            if (tokens[3].length() == 3) {
                seconde = Integer.parseInt(tokens[3].substring(0, 1));
                centieme = Integer.parseInt(tokens[3].substring(1, 3));
            } else if (tokens[3].length() == 4) {
                seconde = Integer.parseInt(tokens[3].substring(0, 2));
                centieme = Integer.parseInt(tokens[3].substring(2, 4));
            } else {
                seconde = 0;
                centieme = Integer.parseInt(tokens[3]);
            }
        }


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
        String[] tokens2 = fileName.split("_");
        
        if(tokens[0].equals("1") || tokens[0].equals("2") ){
            direction = tokens2[1]+" "+tokens2[2];
        }
        else {
            direction = tokens[0];
        }

        String date = tokens[1]+"/10/2022";
        String vitesse = tokens[4].split("=")[1];

        context.write(key, new Sensor(tokens2[0], direction, date , heure, minute , seconde, centieme, 
        Double.parseDouble(vitesse), tokens[6] ));

    }
}
