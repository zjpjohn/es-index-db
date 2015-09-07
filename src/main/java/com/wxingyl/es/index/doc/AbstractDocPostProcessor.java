package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.IndexTypeDesc;

/**
 * Created by xing on 15/9/7.
 * abstract DocPostProcessor
 */
public abstract class AbstractDocPostProcessor implements DocPostProcessor {

    private ThreadLocal<IndexTypeDesc> postEvn = new ThreadLocal<>();

    @Override
    public void startPost(IndexTypeDesc type) {
        postEvn.set(type);
    }

    @Override
    public void endPost(IndexTypeDesc type) {
        postEvn.remove();
    }

    protected IndexTypeDesc getPostEvn() {
        return postEvn.get();
    }
}
