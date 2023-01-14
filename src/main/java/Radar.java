import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Radar implements Writable, Cloneable{
    private int SENS ;
    private int JOUR;
    private int HEURE , MINUTE;
    private int SECONDE , CENTIEME;
    private String VITESSE;
    private String SER;
    private String TYPE;
    public Radar(){

    }

    public Radar(int SENS, int JOUR, int HEURE, int MINUTE, int SECONDE, int CENTIEME, String VITESSE, String SER, String TYPE) {
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
        dataOutput.writeInt(SENS);
        dataOutput.writeInt(JOUR);
        dataOutput.writeInt(HEURE);
        dataOutput.writeInt(MINUTE);
        dataOutput.writeInt(SECONDE);
        dataOutput.writeInt(CENTIEME);
        dataOutput.writeUTF(VITESSE);
        dataOutput.writeUTF(SER);
        dataOutput.writeUTF(TYPE);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        SENS = dataInput.readInt();
        JOUR = dataInput.readInt();
        HEURE = dataInput.readInt();
        MINUTE = dataInput.readInt();
        SECONDE = dataInput.readInt();
        CENTIEME= dataInput.readInt();
        VITESSE = dataInput.readUTF();
        SER = dataInput.readUTF();
        TYPE = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return SENS +";" + JOUR +";" + HEURE +";" + MINUTE + ";" + SECONDE+";" +CENTIEME + ";" + VITESSE + ";" + SER+ ";" + TYPE;
    }
}
