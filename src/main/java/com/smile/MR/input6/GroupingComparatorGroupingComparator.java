package com.smile.MR.input6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author smi1e
 * Date 2019/10/3 21:09
 * Description
 */
public class GroupingComparatorGroupingComparator extends WritableComparator {
    public GroupingComparatorGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean orderBeanA = (OrderBean) a;
        OrderBean orderBeanB = (OrderBean) b;
        if (orderBeanA.getOrderId() > orderBeanB.getOrderId()) {
            return 1;
        } else if (orderBeanA.getOrderId() < orderBeanB.getOrderId()) {
            return -1;
        } else {
            return 0;
        }
    }
}
