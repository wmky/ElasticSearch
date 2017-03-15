package com.juanpi.bi.es;

/**
 * Created by wmky_kk on 2017/3/15.
 */
public class Student {
    private String name;
    private int sex;
    private int number;

    public Student(String name, int sex, int number) {
        this.name = name;
        this.sex = sex;
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
