package com.smile.MR.input2.flowOrder;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 19:36
 * Description
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private long upflow;
    private long downflow;
    private long sumflow;

    public OrderBean() {
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    @Override
    public String toString() {
        return upflow + "\t" + "\t" + downflow + "\t" + sumflow;
    }

    @Override
    public int compareTo(OrderBean o) {
        return sumflow > o.sumflow ? 1 : -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upflow = dataInput.readLong();
        downflow = dataInput.readLong();
        sumflow = dataInput.readLong();
    }
}
