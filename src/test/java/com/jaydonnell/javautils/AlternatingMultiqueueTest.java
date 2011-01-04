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
}
