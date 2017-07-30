package org.saliya.ndssl.multilinearscan.test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

/**
 * Saliya Ekanayake on 7/23/17.
 */
public class MathTests {
    public static void main(String[] args) {
        /*double epsilon = 0.1;
        double probSuccess = 0.2;
        int iter = (int) Math.round(Math.log(epsilon) / Math.log(1 - probSuccess));
        System.out.println(iter);


        BigInteger zero = BigInteger.ZERO;
        System.out.println(zero);
        System.out.println(zero.setBit(3));

        System.out.println("++++");
        Random rand = new Random();
        byte[] b = new byte[5];
        rand.nextBytes(b);
        System.out.println(Arrays.toString(b));

        BigInteger y = BigInteger.valueOf(7L);
        System.out.println(y.toString(2));*/

//        randomTest();
        integerBitCount();
    }

    static void integerBitCount(){
        int x = -7;
        System.out.println(Integer.bitCount(x));
        x = 7;
        System.out.println(Integer.bitCount(x));
    }


    static void randomTest() {
        long seed = 234567890;
        Random rand = new Random(seed);
        int size = 5;
        byte[] bytes = new byte[size];
        rand.nextBytes(bytes);

        System.out.println(rand.nextLong());
        rand = new Random(seed);
        System.out.println(rand.nextLong());

    }
}
