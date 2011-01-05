package com.jaydonnell.javautils;

import com.google.common.collect.LinkedListMultimap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** 
 * @author ddonnell
 */
public class AlternatingMultiqueue<K,E> {

    private final int capacity;
    private final AtomicInteger count = new AtomicInteger(0);
    // TODO is LinkedListMultimap the right choice?
    private final LinkedListMultimap<K, E> multiMap = LinkedListMultimap.create();
    private final LinkedList<K> keys = new LinkedList<K>();
    private int currentKey = 0;


    private final ReentrantLock dequeueLock = new ReentrantLock();
    private final Condition notEmpty = dequeueLock.newCondition();

    private final ReentrantLock enqueueLock = new ReentrantLock();
    private final Condition notFull = enqueueLock.newCondition();

    public AlternatingMultiqueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
    }

    private void incrementCurrentKey() {
        currentKey += 1;
        if (currentKey >= keys.size())
            currentKey = 0;
    }

    private void insert(K key, E e) {
        multiMap.put(key, e);
        if(!keys.contains(key))
            keys.add(key);
    }

    private E extract() {
        K key = keys.get(currentKey);
        E x = multiMap.get(key).remove(0);

        // - if key is empty after extract we need to remove key from keys
        // TODO do we need to remove it from multimap?
        if (multiMap.get(key).isEmpty()) {
            keys.remove(key);
            currentKey -= 1;
        }
        // - we need to increment currentKey
        incrementCurrentKey();
        return x;
    }

    public E dequeue() throws InterruptedException {
        E x;
        int c = -1;
        //final AtomicInteger count = this.count;
        //final ReentrantLock dequeueLock = this.dequeueLock;
        dequeueLock.lockInterruptibly();
        try {
            try {
                while(count.get() == 0)
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal();
                throw ie;
            }
            x = extract();
            c = count.getAndDecrement();
            if(c > 1)
                notEmpty.signal();
        } finally {
            dequeueLock.unlock();
        }

        // if the queue was full before dequeue we need to signal notFull
        if(c == capacity) {
            final ReentrantLock enqueueLock = this.enqueueLock;
            enqueueLock.lock();
            try {
                notFull.signal();
            } finally {
                enqueueLock.unlock();
            }
        }

        return x;
    }

    public void enqueue(K key, E e) throws InterruptedException {
        if(e == null) throw new NullPointerException();

        //final ReentrantLock enqueueLock = this.enqueueLock;
        //final AtomicInteger count = this.count;

        enqueueLock.lockInterruptibly();
        try {
            try {
                while(count.get() == capacity)
                    notFull.await();
            } catch (InterruptedException ie) {
                notFull.signal();
                throw ie;
            }
            insert(key, e);
            int c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            enqueueLock.unlock();
        }
    }
}
