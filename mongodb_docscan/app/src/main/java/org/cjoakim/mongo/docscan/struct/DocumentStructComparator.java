package org.cjoakim.mongo.docscan.struct;

import java.util.Comparator;

/**
 * Comparator object used to sort a Collection of DocumentStruct objects.
 *
 * Microsoft CosmosDB GBB team: Chris Joakim, Aleksey Savateyev, Emmanuel Deletang, et al.
 */
public class DocumentStructComparator implements Comparator<DocumentStruct> {

    @Override
    public int compare(DocumentStruct ds1, DocumentStruct ds2) {
        return ds2.compareTo(ds1);
    }
}
