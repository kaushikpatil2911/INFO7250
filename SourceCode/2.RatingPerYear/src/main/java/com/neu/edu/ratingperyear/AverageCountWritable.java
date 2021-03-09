/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 *
 * @author kaushikpatil
 */
public class AverageCountWritable implements Writable{

	public double RatingAvg;
	public int count;
	
	public AverageCountWritable(double RatingAvg, int count) {
                super();
                this.RatingAvg = RatingAvg;
                this.count = count;
	}

	public AverageCountWritable() {
            super();
	}

	
    public double getRatingAvg() {
	return RatingAvg;
    }

    public void setRatingAvg(double RatingAvg) {
	this.RatingAvg = RatingAvg;
    }

    public int getCount() {
	return count;
    }

    public void setCount(int count) {
	this.count = count;
    }

    public void readFields(DataInput in) throws IOException {
        RatingAvg = in.readDouble();
        count = in.readInt();
    }
        
    public void write(DataOutput out) throws IOException {
        out.writeDouble(RatingAvg);
        out.writeInt(count);
    }

    @Override
    public String toString() {
	return "Rated=" + count+  "\t"+",Average Rating for Year=" + RatingAvg;
    }
}