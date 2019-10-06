package com.smile.MR.input11;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author smi1e
 * Date 2019/10/5 16:08
 * Description
 */
public class Flow implements WritableComparable<Flow> {

    private String phoneNum;
    private Long upflow;
    private Long downflow;
    private Long sumflow;

    @Override
    public String toString() {
        return phoneNum + "\t" + upflow + "\t" + downflow + "\t" + sumflow;
    }

    @Override
    public int compareTo(Flow o) {
        return -sumflow.compareTo(o.sumflow);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNum);
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNum = dataInput.readUTF();
        upflow = dataInput.readLong();
        downflow = dataInput.readLong();
        sumflow = dataInput.readLong();
    }

    public Flow() {
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public Long getUpflow() {
        return upflow;
    }

    public void setUpflow(Long upflow) {
        this.upflow = upflow;
    }

    public Long getDownflow() {
        return downflow;
    }

    public void setDownflow(Long downflow) {
        this.downflow = downflow;
    }

    public Long getSumflow() {
        return sumflow;
    }

    public void setSumflow(Long sumflow) {
        this.sumflow = sumflow;
    }
}
