/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author kaushikpatil
 */
public class WordFrequency implements Writable, WritableComparable<WordFrequency>{
    
    private String word;
    private int frequency;
    
    public WordFrequency() {
        super();
    }
    
    public WordFrequency(int frequency, String word) {
        super();
        this.frequency = frequency;
        this.word = word;
    }
    
    @Override
    
    public String toString() {
        return "WordFrequency [word=" + word + ", frequency=" + frequency + "]";
    }
    
    public String getWord() {
        return word;
    }
    
    public void setWord(String word) {
        this.word = word;
    }
    
    public int getFrequency() {
        return frequency;
    }
    
    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
	result = prime * result + frequency;
	result = prime * result + ((word == null) ? 0 : word.hashCode());
	return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        
        if (this == obj)
            return true;
        
        if (obj == null)
            return false;
        
        if (getClass() != obj.getClass())
            return false;
        
        WordFrequency other = (WordFrequency) obj;
        if (frequency != other.frequency)
            return false;
        
        if (word == null) {
            if (other.word != null)
                return false;
            
        } else if (!word.equals(other.word))
            return false;
        
        return true;
    }
    
    public void readFields(DataInput in) throws IOException {
        
        frequency = in.readInt();
	word = in.readUTF();
    }
    
    public void write(DataOutput out) throws IOException {
        
        out.writeInt(frequency);
        out.writeUTF(word);
    }
    
    public int compareTo(WordFrequency o) {
        
        int result = -1;
        
        if(frequency >  o.frequency) {
            result = 1;
        }
        
        if(frequency == o.frequency) {
            result = 0;
        }
        
        if(result == 0) {
            result  = word.compareTo(o.word);
        }
        
        return result;
    }
}