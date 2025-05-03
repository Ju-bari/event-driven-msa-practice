package com.jh.jasypt;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;

public class TestJasypt {

    public static void main(String[] args) {

        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setAlgorithm("PBEWITHHMACSHA512ANDAES_256");
        encryptor.setPassword("jasypt_test123");
        encryptor.setIvGenerator(new RandomIvGenerator());

        String result = encryptor.encrypt("spring_pwd!123");

        System.out.println(result);
        System.out.println(encryptor.decrypt(result));
    }

}
