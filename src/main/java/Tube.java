import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tube implements Writable, Cloneable{
    private int voie ;
    private String date;
    private int heure, minute, seconde, centieme, vitesse;
    private String catégorie;
    public Tube(){

    }


    public Tube(int voie, String date, int heure, int minute, int seconde, int centieme, int vitesse,
            String catégorie) {
        this.voie = voie;
        this.date = date;
        this.heure = heure;
        this.minute = minute;
        this.seconde = seconde;
        this.centieme = centieme;
        this.vitesse = vitesse;
        this.catégorie = catégorie;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(voie);
        dataOutput.writeUTF(date);
        dataOutput.writeInt(heure);
        dataOutput.writeInt(minute);
        dataOutput.writeInt(seconde);
        dataOutput.writeInt(centieme);
        dataOutput.writeInt(vitesse);
        dataOutput.writeUTF(catégorie);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        voie = dataInput.readInt();
        date = dataInput.readUTF();
        heure = dataInput.readInt();
        minute = dataInput.readInt();
        seconde = dataInput.readInt();
        centieme = dataInput.readInt();
        vitesse = dataInput.readInt();
        catégorie = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return voie +";" + date +";" + heure + ";" + minute + ";" + seconde + ";" + centieme + ";" + vitesse + ";" + catégorie;
    }

}
