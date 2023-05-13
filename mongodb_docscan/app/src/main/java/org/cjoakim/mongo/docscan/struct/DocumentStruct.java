package org.cjoakim.mongo.docscan.struct;

import org.bson.RawBsonDocument;

/**
 * Instances of this class are simple data structure which contain a RawBsonDocument
 * and its size as an int.  The Comparable interface is implemented so instances can be
 * sorted by size.
 *
 * Microsoft CosmosDB GBB team: Chris Joakim, Aleksey Savateyev, Emmanuel Deletang, et al.
 */
public class DocumentStruct implements Comparable<DocumentStruct> {

    // Instance variables
    private int documentSize;
    private RawBsonDocument document;

    public DocumentStruct() {
        super();
    }

    public DocumentStruct(int size, RawBsonDocument doc) {
        super();
        documentSize = size;
        document = doc;
    }

    public int getDocumentSize() {
        return documentSize;
    }

    public void setDocumentSize(int documentSize) {
        this.documentSize = documentSize;
    }

    public RawBsonDocument getDocument() {
        return document;
    }

    public void setDocument(RawBsonDocument document) {
        this.document = document;
    }

    public int compareTo(DocumentStruct anotherInstance) {
        if (this.documentSize > anotherInstance.getDocumentSize()) {
            return 1;
        }
        if (this.documentSize < anotherInstance.getDocumentSize()) {
            return -1;
        }
        return 0;
    }
}
