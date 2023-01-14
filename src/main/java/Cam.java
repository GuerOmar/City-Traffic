import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

public class Cam implements Writable, Cloneable{
    private long id ;
    private String categorie;
    private String date , time;
    private boolean isCyclist;
    private String direction;
    public Cam(){

    }

    public Cam(long id, String categorie,String date,String time, boolean isCyclist, String direction) {
        this.id = id;
        this.categorie = categorie;
        this.date = date;
        this.time = time;
        this.isCyclist = isCyclist;
        this.direction = direction;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(categorie);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(time);
        dataOutput.writeUTF(direction);
        dataOutput.writeBoolean(isCyclist);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        categorie = dataInput.readUTF();
        date = dataInput.readUTF();
        time = dataInput.readUTF();
        direction = dataInput.readUTF();
        isCyclist = dataInput.readBoolean();
    }

    @Override
    public String toString() {
        return id +";" + categorie +";" + date + ";" + time + ";" + direction + ";" + isCyclist;
    }
}
