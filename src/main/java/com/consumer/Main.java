package com.consumer;

public class Main {
    public static void main(String[] args) {
        new Thread(new Consumer(100000)).start();
    }
}