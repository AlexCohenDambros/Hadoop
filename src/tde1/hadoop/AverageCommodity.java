package tde1.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AverageCommodity implements WritableComparable<AverageCommodity> {

    private int n;
    private double soma;


    public AverageCommodity() {
    }

    public AverageCommodity(int n, double soma) {
        this.n = n;
        this.soma = soma;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getSoma() {
        return soma;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    @Override
    public int compareTo(AverageCommodity o) { // comparar o objeto this com o "o" como parametro

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
        dataOutput.writeInt(n); // MESMA ORDEM QUE O READ
        dataOutput.writeDouble(soma);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        n = dataInput.readInt(); // SEGUIR A ORDEM DO WRITE
        soma = dataInput.readDouble();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AverageCommodity that = (AverageCommodity) o;
        return n == that.n && Double.compare(that.soma, soma) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, soma);
    }
}

