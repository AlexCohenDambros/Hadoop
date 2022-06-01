package tde1.questao7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Q7Writable implements WritableComparable<Q7Writable> {
    String commodity;
    double qty;

    public Q7Writable() {
    }

    public Q7Writable(String commodity, double qty) {
        this.commodity = commodity;
        this.qty = qty;
    }

    @Override
    public int compareTo(Q7Writable o) {
        if (this.hashCode() > o.hashCode()) {
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(commodity + '\n');
        dataOutput.writeDouble(qty);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readLine(); // SEGUIR A ORDEM DO WRITE
        qty = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "Commodity: '" + commodity + '\'' +
                ", Amount=" + qty;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q7Writable that = (Q7Writable) o;
        return qty == that.qty && Objects.equals(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, qty);
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public double getQty() {
        return qty;
    }

    public void setQty(int qty) {
        this.qty = qty;
    }
}

