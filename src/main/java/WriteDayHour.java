import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WriteDayHour extends TableReducer<Text, Text, Text> {

    private String[] destinations;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // something that need to be done at start of reducer
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String[] line = val.toString().split(",");
            if(key.toString().equals("#Date:Heure")){
                destinations= new String[line.length];
                for(int i = 0;i<line.length;i++)
                    destinations[i] = line[i];
            }
            else{
                Put put = new Put(key.toString().getBytes());
                put.addColumn(Bytes.toBytes("day_stats"), Bytes.toBytes("day"), key.toString().split(":")[0].getBytes());
                put.addColumn(Bytes.toBytes("day_stats"), Bytes.toBytes("hour"), key.toString().split(":")[1].getBytes());
                for(int i=0; i<line.length ; i++){
                    put.addColumn(Bytes.toBytes("day_stats"), Bytes.toBytes(destinations[i]), line[i].getBytes());
                    context.write(new Text(key.toString()), put);
                }

            }
        }
    }
}