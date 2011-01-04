package com.jaydonnell.javautils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author ddonnell
 */
public class AlternatingMultiqueue<E> {
    static class Node<E> {
        volatile E item;
        Node<E> next;
        Node(E x) {
            item = x;
        }
    }

    private final int capacity;
    private final AtomicInteger count = new AtomicInteger(0);
    private Node<E> head;
    private Node<E> last;

    private final ReentrantLock dequeueLock = new ReentrantLock();
    private final Condition notEmpty = dequeueLock.newCondition();

    private final ReentrantLock enqueueLock = new ReentrantLock();
    private final Condition notFull = enqueueLock.newCondition();

    public void enqueue(E e) throws InterruptedException {
        if(e == null) throw new NullPointerException();

        final ReentrantLock queueLock = this.enqueueLock;
        final AtomicInteger count = this.count;

        queueLock.lockInterruptibly();
        try {
            try {
                while(count.get() == capacity)
                    notFull.await();
            } catch (InterruptedException ie) {
                notFull.signal();
                throw ie;
            }
        } finally {
            queueLock.unlock();
        }
    }

    //public E dequeue() {
    //    return E;
    //}
}
