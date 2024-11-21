package com.consumer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Consumer implements Runnable {
    private int m_n;
    private BlockingQueue<Message> m_buffer;
    private int m_port = 40000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    class Task implements Runnable {
        private int m_port = 50000;

        public Task() {
        }

        @Override
        public void run() {
            try (Socket s = new Socket("127.0.0.1", m_port);
                    BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-16LE"));
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), "UTF-16LE"))) {
                // s.setSoTimeout(2000);
                bw.write(MAPPER.writeValueAsString(new Object() {
                    @SuppressWarnings("unused")
                    public String name = "consumer";
                    public List<String> tags = new LinkedList<>();
                    {
                        tags.add("number");
                    }
                }));
                bw.flush();

                System.out.println(MAPPER.writeValueAsString(new Object() {
                    @SuppressWarnings("unused")
                    public String name = "consumer";
                    public List<String> tags = new LinkedList<>();
                    {
                        tags.add("number");
                    }
                }));

                String json = "";
                while (!s.isClosed()) {

                    char[] buff = new char[4096];
                    // System.out.println("开始接收素数");

                    do {
                        int len = br.read(buff);
                        if (len == -1)
                            continue;
                        json += String.copyValueOf(buff).substring(0, len);
                        if (json.length() > 1000000) break;
                        Thread.sleep(10);
                    } while (br.ready());

                    // System.out.println("已接收素数");

                    // System.out.println("json:" + json);

                    if (json == "") {
                        Thread.sleep(1000);
                        continue;
                    }

                    // System.out.println('[' + json.substring(0, json.lastIndexOf("},{") + 1) + ']');

                    List<Message> messages = (MAPPER.readValue('[' + json.substring(0, json.lastIndexOf("},{") + 1) + ']', new TypeReference<List<Message>>() {
                    }));

                    json = json.substring(json.lastIndexOf("},{") + 2);

                    // System.out.println("size:" + messages.size());

                    for (Message message : messages) {
                        m_buffer.add(message);
                    }
                }
            } catch (UnknownHostException e) {
                System.out.println("ERROE(Consumer):" + e.getMessage());
            } catch (IOException e) {
                System.out.println("ERROE(Consumer):" + e.getMessage());
            } catch (Exception e) {
                System.out.println("ERROE(Consumer):" + e.getMessage());
            }
        }
    }

    public Consumer(int n) {
        m_n = n;
        m_buffer = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        new Thread(new Task()).start();
        int n = m_n;
        while (m_n > 0) {
            try {
                isPrime(m_buffer.take().value.num);
                --m_n;
            } catch (InterruptedException e) {
                System.out.println("ERROE:" + e.getMessage());
            }
            if (n - m_n >= 1000) {
                System.out.println("run:" + m_n + ",buffer:" + m_buffer.size());
                n = m_n;
            }
        }
        System.out.println("run:" + m_n);

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
