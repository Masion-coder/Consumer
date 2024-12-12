package com.consumer;

public class Main {
    public static void main(String[] args) {
        Consumer consumer1 = new Consumer(1000000);
        consumer1.setName("consumer1");
        Consumer consumer2 = new Consumer(1000000);
        consumer2.setName("consumer2");
        Consumer consumer3 = new Consumer(1000000);
        consumer3.setName("consumer3");
        new Thread(consumer1).start();
        new Thread(consumer2).start();
        new Thread(consumer3).start();
    }
}