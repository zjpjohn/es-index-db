package com.wxingyl.es.util;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by xing on 15/8/24.
 * read write lock util
 */
public class RwLock<T> {

    private ReadWriteLock rwLock;

    private T obj;

    public RwLock(T obj) {
        rwLock = new ReentrantReadWriteLock();
        this.obj = obj;
    }

    public <R> R readOp(Function<T, R> function) {
        rwLock.readLock().lock();
        try {
            return function.apply(obj);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public <R> R writeOp(Function<T, R> function) {
        rwLock.writeLock().lock();
        try {
            return function.apply(obj);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
