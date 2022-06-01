package tde1.questao6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class QuestaoSeis implements WritableComparable<QuestaoSeis> {

    private int n;
    private double valores;
    private double max;
    private double min;

    public QuestaoSeis() {
    }

    public QuestaoSeis(int n, double valores, double max, double min) {
        this.n = n;
        this.valores = valores;
        this.max = max;
        this.min = min;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getValores() {
        return valores;
    }

    public void setValores(double valores) {
        this.valores = valores;
    }


    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    @Override
    public int compareTo(QuestaoSeis o) { // comparar o objeto this com o "o" como parametro

        if(this.hashCode() > o.hashCode()){
            return + 1;
        }
        else if(this.hashCode() < o.hashCode()){
            return - 1;
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        // vai interpretar o N como int e a mesma coisa com a soma
        // MESMA ORDEM QUE O READ
        dataOutput.writeInt(n);
        dataOutput.writeDouble(valores);
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(min);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        n = dataInput.readInt(); // SEGUIR A ORDEM DO WRITE
        valores = dataInput.readDouble();
        max = dataInput.readDouble();
        min = dataInput.readDouble();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }


    @Override
    public String toString() {
        return super.toString();
    }

}

