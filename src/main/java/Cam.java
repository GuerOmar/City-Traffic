import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Cam implements Writable, Cloneable{
    public long id ;
    public String categorie;
    public String date , time;
    public String direction;
    public Cam(){

    }

    public Cam(long id, String categorie,String date,String time, String direction) {
        this.id = id;
        this.categorie = categorie;
        this.date = date;
        this.time = time;
        this.direction = direction;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(categorie);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(time);
        dataOutput.writeUTF(direction);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        categorie = dataInput.readUTF();
        date = dataInput.readUTF();
        time = dataInput.readUTF();
        direction = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return id +","+ categorie +"," + date + "," + time + "," + direction;
    }
}
