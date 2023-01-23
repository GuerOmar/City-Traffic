import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.TableName;

public class WriteToHBase2 {
    private static final String TABLE_NAME = "oguermazi:citytraffic_table";
    static String[] destinations; //IL FAUT CHANGER LE NAMESPACE

    public static class WriteReducer extends TableReducer<Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // something that need to be done at start of reducer
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] line = val.toString().split(",");
                if(key.toString().equals("Date:Heure")){
                    destinations= line ;
                }
                else{
                    Put put = new Put(key.toString().getBytes());
                    put.addColumn(Bytes.toBytes("day_stats"), Bytes.toBytes("day"), key.toString().split(":")[0].getBytes());
                    put.addColumn(Bytes.toBytes("day_stats"), Bytes.toBytes("hour"), key.toString().split(":")[1].getBytes());
                    for(int i=0; i<line.length ; i++){
                        put.addColumn(Bytes.toBytes("day_stats"), Bytes.toBytes(destinations[i]), line[i].getBytes());
                    }

                    context.write(new Text(key.toString()), put);
                }
            }
        }
    }

    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
    public static void createTable(Connection connect) {
        try {
            final Admin admin = connect.getAdmin();
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
            ColumnFamilyDescriptorBuilder fam = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("day_stats"));
            tableDescriptorBuilder.setColumnFamily(fam.build());
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            createOrOverwrite(admin, tableDescriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void main (String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "Write to HBase example");
        job.setJarByClass(WriteToHBase.class);
        //create the table (sequential part)
        Connection connection = ConnectionFactory.createConnection(conf);
        createTable(connection);
        //input from HDFS file
        FileInputFormat.addInputPath(job, new Path("/user/oguermazi/hour_reducer_data/part-r-00000"));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //output to an HBase table
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, WriteReducer.class, job);
        job.waitForCompletion(true);
    }
}
