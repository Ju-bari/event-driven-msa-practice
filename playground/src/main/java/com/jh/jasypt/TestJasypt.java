package com.jh.jasypt;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;

public class TestJasypt {

    public static void main(String[] args) {

        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setAlgorithm("PBEWITHHMACSHA512ANDAES_256");
        // =========   조심   ===========
        encryptor.setPassword("");
        // =========   조심   ===========
        encryptor.setIvGenerator(new RandomIvGenerator());

        // =========   조심   ===========
        String result = encryptor.encrypt("");
        // =========   조심   ===========

        System.out.println(result);
        System.out.println(encryptor.decrypt(result));
    }

}
