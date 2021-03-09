/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.topproducts;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author kaushikpatil
 */
public class TopComparator extends WritableComparator {
    
    protected TopComparator() {
        super(FloatWritable.class, true);
    }
    
    public int compare(WritableComparable f1, WritableComparable f2) {
        
        FloatWritable fw1 = (FloatWritable) f1;
        FloatWritable fw2 = (FloatWritable) f2;
        int result = fw1.get() < fw2.get() ? 1 : fw1.get() == fw2.get() ? 0 : -1;
        return result;
    }
}