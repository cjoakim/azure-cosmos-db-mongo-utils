package org.cjoakim.mongo.docscan.struct;

import org.cjoakim.mongo.docscan.App;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

/**
 * Instances of this class are a sortable Vector of DocumentStruct objects.
 * By default the size is 10, sorted from largest to smallest, with the smaller
 * documents "falling off" the end of the Vector.
 *
 * Microsoft CosmosDB GBB team: Chris Joakim, Aleksey Savateyev, Emmanuel Deletang, et al.
 */
public class DocumentStructCollection {

    // Instance variables
    private int limit = 10;
    private Vector<DocumentStruct> vector = new Vector<DocumentStruct>();

    public DocumentStructCollection() {
        super();
    }

    public DocumentStructCollection(int limit) {
        super();
        if (limit > 1) {
            if (limit < 100) {
                this.limit = limit;
            }
        }
    }
    public int getLimit() {
        return limit;
    }
    public Vector<DocumentStruct> getVector() {
        return vector;
    }

    public void add(DocumentStruct ds) {
        if (ds != null) {
            if (vector.size() < limit) {
                vector.add(ds);
                if (vector.size() == limit) {
                    sortAndPrune();
                }
            }
            else {
                DocumentStruct lastDS = vector.get(vector.size() - 1);
                if (ds.getDocumentSize() > lastDS.getDocumentSize()) {
                    vector.add(ds);
                    sortAndPrune();
                }
            }
        }
    }

    private void sortAndPrune() {
        Collections.sort(vector, new DocumentStructComparator());
        vector.setSize(limit);
    }

    public void dump() {
        log("DocumentStructCollection dump:");
        log("vector size: " + vector.size());
        for (int i = 0; i < vector.size(); i++) {
            log("  element " + i + ": " + vector.get(i).getDocumentSize());
        }
    }

    public ArrayList<Integer> getSizesArray() {
        ArrayList<Integer> sizes = new ArrayList<Integer>();
        for (int i = 0; i < vector.size(); i++) {
            sizes.add(vector.get(i).getDocumentSize());
        }
        return sizes;
    }

    public ArrayList<String> getJsonPrefixesArray() {
        ArrayList<String> prefixes = new ArrayList<String>();
        for (int i = 0; i < vector.size(); i++) {
            String json = vector.get(i).getDocument().toJson();
            if (json.length() < App.JSON_PREFIX_LENGTH) {
                prefixes.add(json);
            }
            else {
                prefixes.add(json.substring(0, App.JSON_PREFIX_LENGTH));
            }
        }
        return prefixes;
    }

    public static void log(String msg) {
        System.out.println(msg);
    }
}
