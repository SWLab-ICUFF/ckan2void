package test;

public class TestThread {

    public static void main(String[] args) {
        try {
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(5000l);
                    } catch (InterruptedException ex) {
                    }
                }
            };
            t.start();
            t.join(3000);
            t.stop();
            System.out.println("não passou pelo catch");
        } catch (InterruptedException ex) {
            System.out.println("passou pelo catch");
        }
    }
}
