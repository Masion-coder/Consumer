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
    private String m_name;
    public void setName(String name) {
        m_name = name;
    }

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
                {
                    String str = MAPPER.writeValueAsString(new Object() {
                        @SuppressWarnings("unused")
                        public String name = m_name;
                        public List<String> tags = new LinkedList<>();
                        {
                            tags.add("number");
                        }
                    });
                    bw.write(str);
                    bw.flush();

                    System.out.println(str);
                }

                String json = "";

                int m = 0;
                while (m_n - m > 0 && !s.isClosed()) {

                    char[] buff = new char[4096];

                    do {
                        int len = br.read(buff);
                        if (len == -1)
                            continue;
                        json += String.copyValueOf(buff).substring(0, len);
                        if (json.length() > 10000)
                            break;
                        Thread.sleep(10);
                    } while (br.ready());

                    if (json.replaceAll(" ", "").equals("")) {
                        Thread.sleep(1000);
                        continue;
                    }

                    List<Message> messages = new LinkedList<>();

                    if (json.lastIndexOf("},{") != -1) {
                        messages.addAll(MAPPER.readValue('[' + json.substring(0, json.lastIndexOf("},{") + 1) + ']',
                                new TypeReference<List<Message>>() {
                                }));

                        json = json.substring(json.lastIndexOf("},{") + 2);
                    }

                    if (!br.ready() && !json.equals("")) {
                        messages.addAll((MAPPER.readValue('[' + json.substring(0, json.lastIndexOf(",")) + ']',
                                new TypeReference<List<Message>>() {
                                })));
                        json = "";
                    }

                    m += messages.size();

                    for (Message message : messages) {
                        m_buffer.add(message);
                    }

                }
                s.close();
                System.out.println("取消订阅");
            } catch (UnknownHostException e) {
                System.out.println("ERROE(Consumer):" + e.getMessage());
            } catch (InterruptedException e) {
                System.out.println("ERROE(Consumer):" + e.getMessage());
            } catch (IOException e) {
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
        Thread thread = new Thread(new Task());
        thread.start();
        int m = 0;
        int cnt = 0;
        while (m_n - m > 0) {
            try {
                isPrime(m_buffer.take().value.num);
                m++;
            } catch (InterruptedException e) {
                System.out.println("ERROE:" + e.getMessage());
            }
            if (m - cnt >= 1000) {
                System.out.println(m_name + "剩余:" + (m_n - m) + ",buffer:" + m_buffer.size());
                cnt = m;
            }
        }
        System.out.println("finsh");

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
