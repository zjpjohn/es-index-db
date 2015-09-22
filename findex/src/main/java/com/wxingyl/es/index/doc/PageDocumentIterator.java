package com.wxingyl.es.index.doc;

import java.util.Iterator;

/**
 * Created by xing on 15/9/13.
 * page document iterator
 */
public interface PageDocumentIterator extends Iterator<PageDocument> {

    void startFillIndex();

    void finishFillIndex();
}
