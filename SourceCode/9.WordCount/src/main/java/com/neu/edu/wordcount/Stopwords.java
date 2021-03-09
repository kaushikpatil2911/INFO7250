/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.wordcount;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

/**
 *
 * @author kaushikpatil
 */

public class Stopwords {
    
    public static String[] stopwords = {"B06X9L3MVQ"};
    public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));
    public static Set<String> stemmedStopWordSet = stemStringSet(stopWordSet);
    
    public static boolean isStopword(String word) {
        
        if(word.length() < 2) return true;
        if(word.charAt(0) >= '0' && word.charAt(0) <= '9')
            return true; //remove numbers, "25th", etc
        if(stopWordSet.contains(word)) return true;
        else return false;
    }
    
    public static boolean isStemmedStopword(String word) {
        if(word.length() < 2) return true;
        if(word.charAt(0) >= '0' && word.charAt(0) <= '9') return true; //remove numbers, "25th", etc
        String stemmed = stemString(word);
        if(stopWordSet.contains(stemmed))
            return true;
        if(stemmedStopWordSet.contains(stemmed)) 
            return true;
	if(stopWordSet.contains(word)) 
            return true;
	if(stemmedStopWordSet.contains(word)) 
            return true;
        else 
            return false;
	}
    
    public static String removeStopWords(String string) {
        
        String result = "";
        String[] words = string.split("\\s+");
	for(String word : words) {
            if(word.isEmpty()) continue;
            if(isStopword(string)) continue; //remove stopwords
            result += (word+" ");
        }
        return result;
    }
    
    public static String removeStemmedStopWords(String string) {
        
        String result = "";
        String[] words = string.split("\\s+");
        for(String word : words) {
            if(word.isEmpty()) continue;
            if(isStemmedStopword(word)) continue;
            if(word.charAt(0) >= '0' && word.charAt(0) <= '9') continue; //remove numbers, "25th", etc
            result += (word+" ");
        }
        return result;
    }
    
    public static String stemString(String string) {
        return new Stemmer().stem(string);
    }
    
    public static Set<String> stemStringSet(Set<String> stringSet) {
        Stemmer stemmer = new Stemmer();
        Set<String> results = new HashSet<String>();
        for(String string : stringSet) {
            results.add(stemmer.stem(string));
        }
        return results;
    }
}