package com.smile.MR.input6;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/3 20:27
 * Description
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private long orderId;
    private double price;

    public OrderBean() {
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int compareTo(OrderBean o) {
        if (orderId < o.orderId) {
            return -1;
        } else if (orderId > o.orderId) {
            return 1;
        } else {
            if (price < o.price) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(orderId);
        dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readLong();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
