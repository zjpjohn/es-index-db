package com.wxingyl.es.command;

/**
 * Created by xing on 15/10/29.
 * consumer interface
 */
public interface DocConsumer {

    void accept(Object parentDoc, RootNode.Node field);

}
