/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.overallrating;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 *
 * @author kaushikpatil
 */
public class AverageCountWritable implements Writable{

    public float RatingAvg;
    public int count;
	
    public AverageCountWritable (float RatingAvg, int count) {
        this.RatingAvg = RatingAvg;
        this.count = count;
    }

	public AverageCountWritable() {
            super();
        }

	
	public float getRatingAvg() {
            return RatingAvg;
	}

	public void setRatingAvg(float RatingAvg) {
            this.RatingAvg = RatingAvg;
	}

	public int getCount() {
            return count;
	}

	public void setCount(int count) {
            this.count = count;
	}
        
        
        public void readFields(DataInput in) throws IOException {
            RatingAvg = in.readFloat();
            count = in.readInt();
	}


	public void write(DataOutput out) throws IOException {
            out.writeFloat(RatingAvg);
            out.writeInt(count);
	}

	@Override
	public String toString() {
            return "Rating =" + count+ "\t" + "Overall Average Rating=" + RatingAvg;
	}
}