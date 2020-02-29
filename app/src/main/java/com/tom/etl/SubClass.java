package com.tom.etl;

import com.tom.count.CountBean;
import org.apache.commons.lang.StringUtils;

import java.util.*;


/**
 * ClassName: Test
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/21 16:13
 */
//public class SubClass extends SuperClass{
//    public int a =2;
//    public SubClass(){
//        System.out.println("a is "+a);
//    }
//    public int getA(){return a;}
//    public static void main(String[] args) {
//        SuperClass aClass = new SuperClass();
//        SuperClass bClass = new SubClass();
//        System.out.println("num1 is "+(aClass.a+bClass.a));
//        System.out.println("num2 is "+(aClass.getA()+ bClass.getA()));
//        System.out.println("num3 is "+(aClass.a+bClass.getA()));
//        System.out.println("num4 is "+(aClass.getA()+ bClass.a));
//        HashMap<String, String> hashMap = new HashMap<>();
//
//        HashSet<String> hashSet = new HashSet<>();
//        hashSet.add("123");
//        for (String s : hashSet) {
//            System.out.println(s);
//        }
//
//
//    }
//}
//
//class SuperClass{
//    public int a;
//    public SuperClass(){
//        a=1;
//        System.out.println("a is "+a);
//    }
//    public int getA(){return a;}
//}

class Test2 {
    private static int index =0;
    public static void add(Byte b) {
        b = b++;
    }

    public static void test() {
        Byte a = 127;
        Byte b = 127;
        add(++a);
        System.out.print(Integer.toBinaryString(a) + "|"+ a);
        add(b);
        System.out.print(b + "");
    }
    public static int test2(){
        int i = 1;
        System.out.println((index++)+"=================");
        test2();
        try {
            return i;
        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }finally {
            i++;
            return i;
        }
    }
    public static void test3(){
        while (true){
            new CountBean();
        }
    }


    public static void main(String[] args) {
//        test();
//        int i = test2();
//        System.out.println("------------"+i+"-------");
//        test3();

//        test2();

        Persion persion = new Persion();
        persion.setName("lisi");
        System.out.println(persion);
        test4(persion);
        System.out.println(persion);

    }
    public static void test4(Persion persion){
        persion=null;
    }
}
class Persion{
    private String name;

    public Persion() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Persion{" +
                "name='" + name + '\'' +
                '}';
    }
}
