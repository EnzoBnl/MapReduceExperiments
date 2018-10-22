package com.enzobnl.submit;

import java.io.IOException;

public class YarnSubmitter {
    public static void main(String[] args) throws IOException {
        String mainClass = "com.enzobnl.wordcountmr.WordCount";
        String jar = "./target/wordcountmr-1.0-SNAPSHOT.jar";
        String in = "pom.xml";
        String out = "output";
        String toCall = String.format("c:/Applications/Hadoop/hadoop-2.7.6/bin/yarn.cmd jar %s %s %s %s", jar, mainClass, in, out);
        System.out.println(toCall);
        Runtime.getRuntime().exec(toCall);
    }
}
