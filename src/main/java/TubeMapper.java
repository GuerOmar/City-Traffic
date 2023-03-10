import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;
import java.lang.reflect.Method;


public class TubeMapper extends Mapper<LongWritable, Text, LongWritable, Sensor> {
    public String[] directions ;
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String tokens[] = value.toString().split(",");
        String direction = "";
        if(key.get() ==0)
            return;
        if (tokens.length < 6) return;
        if(tokens[0].length() == 0 || tokens[1].length() ==0 || tokens[2].length() ==0 || tokens[3].length() == 0
                || tokens[4].length() ==0 || tokens[5].length() ==0  )
            return ;

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
        if (tokens2.length >=3)
            direction = tokens2[1]+" "+tokens2[2];
        else direction = tokens2[1];
        
        String date = tokens[0].split("/")[0]+"/"+tokens[0].split("/")[1]+"/20"+tokens[0].split("/")[2];

        int heure = Integer.parseInt(tokens[1].split(":")[0]);
        int minute = Integer.parseInt(tokens[1].split(":")[1]) ;
        context.write(key,new Sensor(tokens2[0],direction, date, heure, minute, Integer.parseInt(tokens[2]),
        Integer.parseInt(tokens[3]), Double.parseDouble(tokens[4]), tokens[5] ));

    }
}
