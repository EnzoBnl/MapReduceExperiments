package com.enzobnl.playground;
import java.util.*;

/**
 * Auto-generated code below aims at helping you parse
 * the standard input according to the problem statement.
 **/

class Solution {
    public static String createString(Map<Character, List<String>> map, String T, int L, int H){
        StringBuilder T_ = new StringBuilder();
        for(char c: T.toCharArray()){
            char c_low = String.valueOf(c).toLowerCase().charAt(0);
            if(map.containsKey(c_low)){
                T_.append(c_low);
            }
            else{
                T_.append('?');
            }
        }
        T = T_.toString();
        StringBuilder res = new StringBuilder();
        for(int layer=0; layer<H;layer++){
            for(char letter: T.toCharArray()){
                res.append(map.get(letter).get(layer));
            }
            res.append("\n");
        }
        return res.toString();
    }

    public static void main(String args[]) {
        Scanner in = new Scanner(System.in);
        int L = in.nextInt();
        int H = in.nextInt();
        if (in.hasNextLine()) {
            in.nextLine();
        }
        String T = in.nextLine();
        Map<Character, List<String>> map = new HashMap<Character, List<String>>();
        boolean firstRow=true;
        char[] chars = {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','?'};
        for (int i = 0; i < H; i++) {
            String row = in.nextLine();
            for(int index = 0; index < chars.length*L; index+=L) {
                char c = chars[index/L];
                if (firstRow) {
                    List<String> layers = new ArrayList<String>();
                    layers.add(row.substring(index, index+L));
                    map.put(c, layers);
                } else {
                    map.get(c).add(row.substring(index, index+L));
                }
            }
            if(firstRow){
                firstRow = false;
            }
        }

        // Write an action using System.out.println()
        // To debug: System.err.println("Debug messages...");
        System.out.println(Solution.createString(map, T, L, H));
    }
}