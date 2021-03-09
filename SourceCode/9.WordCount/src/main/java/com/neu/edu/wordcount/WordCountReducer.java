/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.wordcount;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author kaushikpatil
 */
public class WordCountReducer extends Reducer<Text, WordFrequency, Text,WordFrequency>{
    
    @Override
    
    protected void reduce(Text key, Iterable<WordFrequency> values, Context context) throws IOException, InterruptedException {
        
        Map<String, Integer> frequencyMap = new TreeMap<String,Integer>();
        
        for(WordFrequency wf : values) {
            
            if(!frequencyMap.containsKey(wf.getWord())) {
                frequencyMap.put(wf.getWord(), wf.getFrequency());
            }
            else {
                
                int freq = frequencyMap.get(wf.getWord());
                frequencyMap.put(wf.getWord(), freq + 1);
            }
        }
        
        for(Map.Entry<String, Integer> entry : frequencyMap.entrySet()) {
            
            WordFrequency wf1 = new WordFrequency();
            wf1.setFrequency(entry.getValue());
            wf1.setWord(entry.getKey());
            context.write(key, wf1);
        }
    }
}