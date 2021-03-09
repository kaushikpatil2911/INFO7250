/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author kaushikpatil
 */
public class YearComparator extends WritableComparator{

    protected YearComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
	public int compare(WritableComparable a, WritableComparable b) {
		
            CompositeKeyWritable ck1 = (CompositeKeyWritable) a;
            CompositeKeyWritable ck2 = (CompositeKeyWritable) b;
            return ck1.getProductId().compareTo(ck2.getProductId());
        }
}