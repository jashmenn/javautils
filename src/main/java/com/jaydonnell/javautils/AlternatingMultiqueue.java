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

    public synchronized E dequeue() throws InterruptedException, IllegalStateException {
        E x;
        int c = -1;
        if (count.get() == 0)
            throw new IllegalStateException();
        x = extract();
        c = count.getAndDecrement();

        return x;
    }

    public synchronized void enqueue(K key, E e) throws InterruptedException {
        if(e == null) throw new NullPointerException();

        if(count.get() == capacity)
            throw new IllegalStateException();

        insert(key, e);
        int c = count.getAndIncrement();
    }

    /* 
     * Retrieves and removes the head of this queue, or null if this queue is empty.
     */
    public synchronized E poll() throws InterruptedException {
        try {
            return this.dequeue();
        } catch (IllegalStateException e) {
            return null;
        }
    }


    /* 
     * Retrieves and removes the head of this queue, waiting if no
     * elements are present on this queue.
     *
     * A hack. It sleeps in a loop rather than using proper locking
     * conditions and signals.
     */
    public synchronized E take() throws InterruptedException {
        boolean success = false;
        E result = null;
        while(!success) {
          try {
            result = this.dequeue();
            success = true;
          } catch (IllegalStateException e) {
              Thread.sleep(100);
          }
        }
        return result;
    }



}
