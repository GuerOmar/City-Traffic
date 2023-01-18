import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;


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
        Path path = ((FileSplit)inputSplit).getPath();
        String fileName = path.getName();
        String[] tokens2 = fileName.split("_");
        direction = tokens2[1]+" "+tokens2[2];
        
        String date = tokens[0].split("/")[0]+"/"+tokens[0].split("/")[1]+"/20"+tokens[0].split("/")[2];

        int heure = Integer.parseInt(tokens[1].split(":")[0]);
        int minute = Integer.parseInt(tokens[1].split(":")[1]) ;
        context.write(key,new Sensor(tokens2[0],direction, date, heure, minute, Integer.parseInt(tokens[2]),
        Integer.parseInt(tokens[3]), Double.parseDouble(tokens[4]), tokens[5] ));

    }
}
