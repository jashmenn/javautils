package com.jaydonnell.javautils;

import org.junit.Test;
import org.junit.Assert;
import java.util.HashSet;

/**
 * @author ddonnell
 */
public class AlternatingMultiqueueTest {

    @Test
    public void testAlternatingDequeque() throws InterruptedException {
        String o = null;
        AlternatingMultiqueue<String, String> queue = new AlternatingMultiqueue<String, String>(10);
        queue.offer("k1", "first");
        queue.offer("k1", "fourth");
        queue.offer("k2", "second");
        queue.offer("k3", "third");

        o = queue.poll();
        Assert.assertTrue("first".equals(o));
        o = queue.poll();
        Assert.assertTrue("second".equals(o));
        o = queue.poll();
        Assert.assertTrue("third".equals(o));
        o = queue.poll();
        Assert.assertTrue("fourth".equals(o));
    }

    @Test
    public void testAlternatingPoll() throws InterruptedException {
        String o = null;
        AlternatingMultiqueue<String, String> queue = new AlternatingMultiqueue<String, String>(10);
        queue.offer("k1", "first");

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
                    queue.offer("k2", "second");
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }

        queue.offer("k1", "first");
        o = queue.take();
        Assert.assertTrue("first".equals(o));

        Runnable enq = new EnqThread();
        enq.run();

        o = queue.take();
        Assert.assertTrue("second".equals(o));
    }

    @Test
    public void testDrainTo() throws InterruptedException {
        String o = null;
        AlternatingMultiqueue<String, String> queue = new AlternatingMultiqueue<String, String>(10);
        queue.offer("k1", "first");
        queue.offer("k1", "fourth");
        queue.offer("k2", "second");
        queue.offer("k3", "third");

       
        HashSet s = new HashSet();
        queue.drainTo(s);

        Assert.assertEquals("Drained collection should have the right number of elements", 4, s.size());
        Assert.assertEquals("AlternatingMultiqueue should be empty", 0, queue.size());

        Assert.assertTrue(s.contains("first"));
        Assert.assertTrue(s.contains("second"));
        Assert.assertTrue(s.contains("third"));
        Assert.assertTrue(s.contains("fourth"));
    }

}
