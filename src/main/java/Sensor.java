import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Sensor implements Writable, Cloneable{
    public String id;
    public String direction ;
    public String date;
    public int heure, minute, seconde, centieme;
    public double vitesse;
    public String categorie;
  

    public Sensor(){  }
    
    public Sensor(String id, String direction, String date, int heure, int minute, int seconde, int centieme, double vitesse,
            String categorie) {
        this.id = id;
        this.direction = direction;
        this.date = date;
        this.heure = heure;
        this.minute = minute;
        this.seconde = seconde;
        this.centieme = centieme;
        this.vitesse = vitesse;
        this.categorie = categorie;
    }


   @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(direction);
        out.writeUTF(date);
        out.writeInt(heure);
        out.writeInt(minute);
        out.writeInt(seconde);
        out.writeInt(centieme);
        out.writeDouble(vitesse);
        out.writeUTF(categorie);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
            id = in.readUTF();
            direction = in.readUTF();
            date = in.readUTF();
            heure = in.readInt();
            minute = in.readInt();
            seconde = in.readInt();
            centieme = in.readInt();
            vitesse = in.readDouble();
            categorie = in.readUTF();        
    }


    @Override
    public String toString() {
        if(vitesse == 0){
            return id + "," + direction +"," + date +"," + heure + "," + minute + "," + seconde + "," + centieme + "," + "," + categorie;
        }
        return id + "," + direction +"," + date +"," + heure + "," + minute + "," + seconde + "," + centieme + "," + vitesse + "," + categorie;
    }
}
