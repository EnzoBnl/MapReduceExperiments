// Java code​​​​​​​‌‌​‌​‌​‌​​‌‌‌‌‌‌​​​‌‌‌​​ below
package com.enzobnl.playground;
import java.util.*;


public class CodinGame{
    public static void main(String[] args){
        ArrayList<Integer> ints = new ArrayList<Integer>();
        ints.add(1);
        ArrayList<Integer> ints_ = new ArrayList<Integer>(ints);
        ints.remove(0);
        System.out.println(ints);
    }
}