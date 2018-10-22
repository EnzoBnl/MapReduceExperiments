package com.enzobnl.playground;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SandBox {
    public static void main(String[] args){
        Pattern pattern = Pattern.compile("[ \t]*(<[^(> )]+>?).*");
        Matcher matcher = pattern.matcher("\t \t  <artifactId>hadoop-hdfs</artifactId>8787");
        if(matcher.matches()){
            System.out.println(matcher.group(1));
//            System.out.println(matcher.group(2));
        }
    }
}
