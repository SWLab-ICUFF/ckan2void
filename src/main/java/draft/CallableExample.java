package draft;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CallableExample {

    public static class WordLengthCallable
            implements Callable {

        private final String word;

        public WordLengthCallable(String word) {
            this.word = word;
        }

        @Override
        public Integer call() {
            return Integer.valueOf(word.length());
        }
    }

    public static void main(String args[]) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        Set<Future<Integer>> set = new HashSet<>();
        for (String word : args) {
            Callable<Integer> callable = new WordLengthCallable(word);
            Future<Integer> future = pool.submit(callable);
            set.add(future);
        }
        int sum = 0;
        for (Future<Integer> future : set)
            sum += future.get(1000l, TimeUnit.MILLISECONDS);
        System.out.printf("The sum of lengths is %s%n", sum);
        System.exit(sum);
    }
}
