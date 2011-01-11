package com.jaydonnell.javautils;

import org.junit.Test;
import org.junit.Assert;

/**
 * @author ddonnell
 */
public class AlternatingMultiqueueTest {

    @Test
    public void testAlternatingDequeque() throws InterruptedException {
        String o = null;
        AlternatingMultiqueue<String, String> queue = new AlternatingMultiqueue<String, String>(10);
        queue.enqueue("k1", "first");
        queue.enqueue("k1", "fourth");
        queue.enqueue("k2", "second");
        queue.enqueue("k3", "third");

        o = queue.dequeue();
        Assert.assertTrue("first".equals(o));
        o = queue.dequeue();
        Assert.assertTrue("second".equals(o));
        o = queue.dequeue();
        Assert.assertTrue("third".equals(o));
        o = queue.dequeue();
        Assert.assertTrue("fourth".equals(o));
    }

    @Test
    public void testAlternatingPoll() throws InterruptedException {
        String o = null;
        AlternatingMultiqueue<String, String> queue = new AlternatingMultiqueue<String, String>(10);
        queue.enqueue("k1", "first");

        o = queue.poll();
        Assert.assertTrue("first".equals(o));
        o = queue.poll();
        Assert.assertTrue(o == null);
    }

    @Test
    public void testAlternatingTake() throws InterruptedException {
        String o = null;
        final AlternatingMultiqueue<String, String> queue = new AlternatingMultiqueue<String, String>(10);

        class EnqThread extends Thread {
            public void run() {
                try {
                    Thread.sleep(500);
                    queue.enqueue("k2", "second");
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }

        queue.enqueue("k1", "first");
        o = queue.take();
        Assert.assertTrue("first".equals(o));

        Runnable enq = new EnqThread();
        enq.run();

        o = queue.take();
        Assert.assertTrue("second".equals(o));
    }

}
