import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Radar implements Writable, Cloneable{
    public String SENS ;
    public int JOUR;
    public int HEURE , MINUTE;
    public int SECONDE , CENTIEME;
    public double VITESSE;
    public int SER;
    public String TYPE;
    public Radar(){

    }

    public Radar(String SENS, int JOUR, int HEURE, int MINUTE, int SECONDE, int CENTIEME, double VITESSE, int SER, String TYPE) {
        this.SENS = SENS;
        this.JOUR = JOUR;
        this.HEURE = HEURE;
        this.MINUTE = MINUTE;
        this.SECONDE = SECONDE;
        this.CENTIEME = CENTIEME;
        this.VITESSE = VITESSE;
        this.SER = SER;
        this.TYPE = TYPE;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(SENS);
        dataOutput.writeInt(JOUR);
        dataOutput.writeInt(HEURE);
        dataOutput.writeInt(MINUTE);
        dataOutput.writeInt(SECONDE);
        dataOutput.writeInt(CENTIEME);
        dataOutput.writeDouble(VITESSE);
        dataOutput.writeInt(SER);
        dataOutput.writeUTF(TYPE);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        SENS = dataInput.readUTF();
        JOUR = dataInput.readInt();
        HEURE = dataInput.readInt();
        MINUTE = dataInput.readInt();
        SECONDE = dataInput.readInt();
        CENTIEME= dataInput.readInt();
        VITESSE = dataInput.readDouble();
        SER = dataInput.readInt();
        TYPE = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return SENS +";" + JOUR +";" + HEURE +";" + MINUTE + ";" + SECONDE+";" +CENTIEME + ";" + VITESSE + ";" + SER+ ";" + TYPE;
    }
}
