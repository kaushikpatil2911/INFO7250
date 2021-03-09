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
public class SecSortingComparator extends WritableComparator{

	protected SecSortingComparator() {
            super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
            CompositeKeyWritable ck1 = (CompositeKeyWritable) a;
            CompositeKeyWritable ck2 = (CompositeKeyWritable) b;
		
            int result = 0;
		
            if(ck1.getYear() > ck2.getYear()) {
                result = 1;
            } 
            
            else if (ck1.getYear() == ck2.getYear()) {
                result = 0;
            }
            
            else {
                result = -1;
            }
		
            if(result == 0) {
                result = ck1.getProductId().compareTo(ck2.getProductId());
            }
	return result*(-1);
        }
}