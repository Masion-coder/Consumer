package com.consumer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Consumer implements Runnable {
    private int m_n;
    private ThreadPoolExecutor m_pool;
    private BlockingQueue<Long> m_buffer;
    private int m_port = 40000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    class Task implements Runnable {
        private int m_n;

        public Task(int n) {
            m_n = n;
        }

        @Override
        public void run() {
            try (Socket s = new Socket("127.0.0.1", m_port);
                    BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-16LE"));
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), "UTF-16LE"))) {
                s.setSoTimeout(2000);
                bw.write(MAPPER.writeValueAsString(new Object() {
                    @SuppressWarnings("unused")
                    public int n = m_n;
                }));
                bw.flush();

                char[] buff = new char[4096];
                String json = "";

                // System.out.println("开始接收素数");

                do {
                    int len = br.read(buff);
                    if (len == -1)
                        continue;
                    json += String.copyValueOf(buff).substring(0, len);
                    Thread.sleep(10);
                } while (br.ready());

                // System.out.println("已接收素数");

                // System.out.println(json);

                Map<String, Object> objectMap = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
                });

                if (objectMap.containsKey("data")) {
                    List<Long> data = MAPPER.readValue(objectMap.get("data").toString(),
                            new TypeReference<List<Long>>() {
                            });
                    for (Long num : data) {
                        m_buffer.put(num);
                    }
                }
            } catch (UnknownHostException e) {
                System.out.println("ERROE:" + e.getMessage());
            } catch (IOException e) {
                System.out.println("ERROE:" + e.getMessage());
            } catch (Exception e) {
                System.out.println("ERROE:" + e.getMessage());
            }
        }
    }

    public Consumer(int n) {
        m_n = n;
        m_pool = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(8), new ThreadPoolExecutor.DiscardPolicy());
        m_buffer = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        while (m_n > 0) {
            if (m_buffer.isEmpty()) {
                m_pool.execute(new Task(1000));
            } else {
                try {
                    isPrime(m_buffer.take());
                    --m_n;
                    System.out.println(m_n);
                } catch (InterruptedException e) {
                    System.out.println("ERROE:" + e.getMessage());
                }
            }
        }
    }

    public boolean isPrime(long num) {
        if (num <= 3) {
            return num > 1;
        }
        // 不在6的倍数两侧的一定不是质数
        if (num % 6 != 1 && num % 6 != 5) {
            return false;
        }
        long sqrt = (long) Math.sqrt(num);
        for (long i = 5; i <= sqrt; i += 6) {
            if (num % i == 0 || num % (i + 2) == 0) {
                return false;
            }
        }
        return true;
    }
}
