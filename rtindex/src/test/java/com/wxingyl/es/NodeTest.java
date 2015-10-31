package com.wxingyl.es;

import com.wxingyl.es.command.RootNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xing on 15/10/27.
 * {@link com.wxingyl.es.command.RootNode} test
 */
public class NodeTest {

    @Test
    public void test() {
        RootNode rootNode = new RootNode();
        List<RootNode.Node> list = new ArrayList<>();
        list.add(rootNode.addNode("xing"));
        list.add(rootNode.addNode("xing.wang"));
        list.add(rootNode.addNode("xing.xing.wang"));
        list.add(rootNode.addNode("xing.xing.shao"));
        String rootStr = rootNode.allNodes().toString();
        String listStr = list.toString();
        Assert.assertTrue("rootNode: " + rootStr + ", list: " + listStr, rootStr.equals(listStr));
    }

    @Test
    public void fullNameTest() {
        RootNode rootNode = new RootNode();
        RootNode.Node node = rootNode.addNode(" ");
        Assert.assertNull(node);
        node = rootNode.addNode("xing");
        Assert.assertEquals(node.fullName(), "xing");
        node = rootNode.addNode("xing.wang");
        Assert.assertEquals(node.fullName(), "xing.wang");
        node = rootNode.addNode("wang.xing.xing");
        Assert.assertEquals(node.fullName(), "wang.xing.xing");
    }
}
