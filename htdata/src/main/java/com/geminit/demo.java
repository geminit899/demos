package com.geminit;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class demo {
    public static void main(String[] args) throws Exception {
        Test test = new Test("tyx", 24, "");

        FileOutputStream out = new FileOutputStream("tmp");
        ObjectOutputStream o = new ObjectOutputStream(out);
//        o.defaultWriteObject();
        o.writeObject(test);
        o.flush();
        o.close();

        FileInputStream in = new FileInputStream("tmp");
        ObjectInputStream s = new ObjectInputStream(in);
//        s.defaultReadObject();
        Test readTest = (Test) s.readObject();
        s.close();

        System.out.println(readTest.addr);
    }

    static class Test implements Serializable {
        private String name;
        private int age;
        private String addr;

        public Test(String name, int age, String addr) {
            this.name = name;
            this.age = age;
            this.addr = addr;
        }
    }
}
