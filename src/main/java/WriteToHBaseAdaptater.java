import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteToHBaseAdaptater extends Configured implements Tool {
    Class reducer;
    private String TABLE_NAME ;

    public WriteToHBaseAdaptater(Class reducer, String name){
        this.reducer = reducer;
        TABLE_NAME = name;
    }

    public void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
    public void createTable(Connection connect) {
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

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "Write to HBase example");
        job.setJarByClass(WriteToHBaseAdaptater.class);
        //create the table (sequential part)
        Connection connection = ConnectionFactory.createConnection(conf);
        createTable(connection);
        //input from HDFS file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //output to an HBase table
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, reducer, job);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main (String[] args) throws Exception {
        System.exit(ToolRunner.run(new WriteToHBaseAdaptater(WriteDayHour.class,"oguermazi:citytraffic_table"), args));
    }

}
