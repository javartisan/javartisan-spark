import org.apache.spark.sql.sources.In;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class AsynThreadFactory implements ThreadFactory {
    private AtomicInteger atomicInteger = new AtomicInteger(1);

    @Override
    public Thread newThread(Runnable r) {

        Thread t = new Thread(r);
        t.setName("asyn-worker-" + atomicInteger.getAndIncrement());
        return t;
    }
}

public class Main {
    public static void main(String[] args) {

        Integer[] data = {1, 2, 3, 4, 5};
        List<String> dataResult = new ArrayList<>();
        Long start = System.currentTimeMillis();

        ExecutorService es = Executors.newCachedThreadPool(new AsynThreadFactory());

        Stream<CompletableFuture> futures = Arrays.stream(data).map(num -> {
            CompletableFuture integerCompletableFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    System.out.println(Thread.currentThread().getName());
                    Thread.sleep(num * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return dataResult.add(num + "-ok");
                // 如果不指定线程池的话，默认使用ForkJoinPool线程池
            }, es);

            return integerCompletableFuture;
        });


        CompletableFuture[] futuresArr = futures.toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futuresArr).join();

        System.out.println(System.currentTimeMillis() - start);

        dataResult.stream().forEach(x -> System.out.println(x));

    }

}
