package com.wxingyl.es.command;

import com.wxingyl.es.util.CommonUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/27.
 * Document field
 */
public class RootNode {

    private Node root;

    public RootNode() {
        root = new Node("", 0, null);
    }

    public Node addNode(String docField) {
        if ((docField = CommonUtils.emptyTrim(docField)) == null) return null;
        String[] fieldSplit = CommonUtils.split(docField, '.');
        return fieldSplit.length == 0 ? null : root.addNode(fieldSplit);
    }

    public List<Node> allNodes() {
        return root.allNodes();
    }

    public class Node {

        final private String field;

        private List<Node> child;

        final private int deep;

        private boolean isEnd;

        private String fullName;

        final private Node parent;

        private Node(String field, int deep, Node parent) {
            this.field = field;
            this.deep = deep;
            this.parent = parent;
        }

        private Node findChildNode(String field) {
            if (child == null) return null;
            for (Node n : child) {
                if (n.field.equals(field)) {
                    return n;
                }
            }
            return null;
        }

        /**
         * this function not have indexArray check
         *
         * @param array split array, array.length > this.level
         * @return add node obj
         */
        private Node addNode(String[] array) {
            if (this.child == null) {
                this.child = new ArrayList<>();
            }
            Node node = findChildNode(array[deep]);
            if (node == null) {
                node = new Node(array[deep], deep + 1, this);
                this.child.add(node);
            }
            //if have more deep child, we need add
            if (array.length > (deep + 1)) {
                node = node.addNode(array);
            }
            if (node.deep == array.length) {
                node.isEnd = true;
            }
            return node;
        }

        public List<Node> allNodes() {
            List<Node> list = new LinkedList<>();
            LinkedList<Node> queue = new LinkedList<>();
            queue.add(this);
            while (!queue.isEmpty()) {
                Node ele = queue.pop();
                if (ele.isEnd) {
                    list.add(ele);
                }
                if (ele.child != null) {
                    queue.addAll(ele.child);
                }
            }
            return list;
        }

        public String field() {
            return field;
        }

        public int deep() {
            return deep;
        }

        public boolean isEnd() {
            return isEnd;
        }

        public String fullName() {
            if (fullName == null) {
                if (parent == null || deep == 1) fullName = field;
                else fullName = parent.fullName() + '.' + field;
            }
            return fullName;
        }

        /**
         * get parent obj which class type is one of {@link Map<String, Object>} and {@link List<Map<String, Object>>}
         *
         * @param source searchHit return document
         * @return return val maybe is null
         */
        public Object getSourceMap(Map<String, Object> source) {
            if (deep == 1) return source;
            return sourceMap(source, deep);
        }

        @SuppressWarnings("unchecked")
        private Object sourceMap(Map<String, Object> source, int endDeep) {
            if (deep == 1) {
                return source.get(field);
            } else {
                Object obj = parent.sourceMap(source, endDeep);
                if (obj == null || deep == endDeep) return obj;
                if (obj instanceof List) {
                    List<Map<String, Object>> list = new ArrayList<>();
                    for (Map<String, Object> m : (List<Map<String, Object>>) obj) {
                        if (m.get(field) != null) {
                            list.add((Map<String, Object>) m.get(field));
                        }
                    }
                    return list.isEmpty() ? null : (list.size() == 1 ? list.get(0) : list);
                } else {
                    return ((Map<String, Object>) obj).get(field);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Node)) return false;

            Node node = (Node) o;

            if (deep != node.deep) return false;
            return field.equals(node.field);
        }

        @Override
        public int hashCode() {
            int result = field.hashCode();
            result = 31 * result + deep;
            return result;
        }

        @Override
        public String toString() {
            return fullName();
        }
    }
}
