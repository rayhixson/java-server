import static java.lang.System.out;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

public class Server {

    final static int port = 8888;
    final static int workerCount = 5;
    final static Pattern acceptableToken = Pattern.compile("^[0-9]{9}$");
    static Boolean running = true;
    static ConcurrentHashMap<String, Boolean> uniqueNums = new ConcurrentHashMap<>();
    static Thread[] workerThreads = new Thread[workerCount];
    static LongAdder uniqueCount = new LongAdder();
    static LongAdder dupCount = new LongAdder();
    static BlockingQueue<Socket> connQueue = new ArrayBlockingQueue<>(workerCount);
    static Writer saveFile = null;

    public static void main(final String[] args) throws Exception {
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });

        // Reporting every 10 secs
        Thread reporter = new Thread() {
            public void run() {
                for (;;) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        out.println(e);
                        System.exit(1);
                    }
                    long newNums, newDups = 0;
                    newNums = uniqueCount.sumThenReset();
                    newDups = dupCount.sumThenReset();

                    out.printf("Received %s unique numbers, %s duplicates. Unique total: %s\n", newNums, newDups,
                            uniqueNums.size());
                }
            }
        };
        reporter.setName("Reporter");
        reporter.start();

        // file to save numbers to
        File f = new File("numbers.log");
        if (f.exists()) {
            f.delete();
        }
        f.createNewFile();
        saveFile = new BufferedWriter(new FileWriter(f));

        // workers to handle requests
        out.println("starting handlers");
        for (int i = 0; i < workerCount; i++) {
            Thread t = new Thread(new TokenHandler());
            workerThreads[i] = t;
            t.setName("Handler-" + i);
            t.start();
        }

        // start a server
        out.println("starting server");
        ServerSocket serverSocket = new ServerSocket(port);
        try {
            while (true) {
                Socket sock = serverSocket.accept();
                if (!connQueue.offer(sock)) {
                    out.println("rejecting client");
                    sock.close();
                } else {
                    out.println("accepted");
                }
            }
        } catch (Exception e) {
            shutdown();
            serverSocket.close();
        }
    } // end main

    static void shutdown() {
        if (running) {
            running = false;
            try {
                for (int i = 0; i < workerThreads.length; i++) {
                    workerThreads[i].interrupt();
                    workerThreads[i].join();
                }
                saveFile.flush();
            } catch (Exception e) {
            }
            out.println("Stopping ...");
        }
    }

    static class TokenHandler implements Runnable {

        @Override
        public void run() {
            try {
                while (true && running) {
                    Socket sock = null;
                    try {
                        sock = connQueue.take();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                        String line = reader.readLine();
                        while (line != null && !line.isEmpty() && running) {
                            if (acceptableToken.matcher(line).matches()) {
                                save(line);
                                line = reader.readLine();
                            } else {
                                out.println("Bad token: " + line);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        out.println("Interuppted: " + e);
                    } finally {
                        try {
                            if (sock != null) {
                                sock.close();
                            }
                        } catch (IOException e) {
                        }
                    }
                }
            } finally {
                out.println("Thread ending: " + Thread.currentThread().getName());
            }
        }

        void save(String token) throws Exception {
            if (uniqueNums.containsKey(token)) {
                dupCount.increment();
            } else {
                uniqueNums.put(token, true);
                uniqueCount.increment();
                saveFile.append(token + System.lineSeparator());
            }
        }
    }
}
