package com.neu.bigdata;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class SortKeyComparator extends WritableComparator {

    protected SortKeyComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable key1 = (IntWritable) a;
        IntWritable key2 = (IntWritable) b;

        int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
        return result;
    }

}
