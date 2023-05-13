package org.cjoakim.mongo.docscan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import org.bson.RawBsonDocument;
import org.bson.json.JsonWriterSettings;
import org.cjoakim.mongo.docscan.struct.DocumentStruct;
import org.cjoakim.mongo.docscan.struct.DocumentStructCollection;
import org.cjoakim.mongo.docscan.util.ScanResults;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * This is the entry-point of the application which is used to scan a MongoDB cluster
 * and identify the largest documents in each container.
 *
 * Microsoft CosmosDB GBB team: Chris Joakim, Aleksey Savateyev, Emmanuel Deletang, et al.
 */

public class App {

    // Constants
    public static final int JSON_PREFIX_LENGTH = 60;

    // Class variables:
    private static MongoClient mongoClient;
    private static ScanResults scanResults = new ScanResults();
    private static boolean scanDocuments = false;
    private static boolean identifyTopTenLargest = false;
    private static JsonWriterSettings jws = JsonWriterSettings.builder().indent(true).build();

    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            log("arg " + i + " -> " + arg);
        }
        try {
            String processType = args[0];
            String clusterLine = args[1];
            // clusterLine is expected to look like this, a vertibar-separated string with no
            // embedded spaces.  First token is the cluster name, second is cluster connection string.
            // <friendly-name>|mongodb+srv://<user>:<password>@<host>
            String[] tokens = clusterLine.split("\\|");
            String clusterName = tokens[0].strip();
            String connString  = tokens[1].strip();

            log("processType: " + processType);
            log("clusterLine: " + clusterLine);
            log("clusterName: " + clusterName);
            log("connString:  " + connString);

            mongoClient = connect(connString);
            scanResults.setClusterName(clusterName);
            scanResults.setSourceUrl(connString);

            if (mongoClient != null) {
                switch (processType) {
                    case "scan_without_sizes":
                        scan();
                        break;
                    case "scan_with_largest":
                        scanDocuments = true;
                        scan();
                    case "scan_with_top_ten_largest":
                        scanDocuments = true;
                        identifyTopTenLargest = true;
                        scan();

                    default:
                        log("undefined processType: " + processType);
                }
            }
        }
        catch (Exception e) {
            scanResults.setExceptionEncountered();
            log(e.getClass().getName() + " -> " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            if (mongoClient != null) {
                log("closing mongoClient");
                mongoClient.close();
                log("closed");
            }
        }
    }

    private static MongoClient connect(String connString) {
        ConnectionString cs = new ConnectionString(connString);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applicationName("mongo_docscan")
                .applyConnectionString(cs)
                .build();

        MongoClient client = MongoClients.create(settings);

        if (client == null) {
            log("MongoClients.create returned null object");
            scanResults.setConnected(false);
        }
        else {
            log("MongoClients.create " + client.getClusterDescription());
            scanResults.setConnected(true);
        }
        return client;
    }

    private static void scan() throws Exception {
        ArrayList<String> databasesList = getDatabasesList();
        scanResults.setDatabases(databasesList);
        log("" + databasesList.size() + " application databases found");

        for (int i = 0; i < databasesList.size(); i++) {
            String dbName = databasesList.get(i);
            MongoDatabase db = mongoClient.getDatabase(dbName);
            iterateContainers(dbName, db, scanDocuments);
        }
        scanResults.finish();
        writeResultsJson();
    }

    private static ArrayList<String> getDatabasesList() {
        MongoIterable<String> iterable = mongoClient.listDatabaseNames();
        MongoCursor<String> dbNamesCursor = iterable.iterator();
        ArrayList<String> databasesList = new ArrayList<String>();

        while (dbNamesCursor.hasNext()) {
            String dbName = dbNamesCursor.next();
            log("mongoClient.listDatabaseNames() found dbName: " + dbName);
            if (includeThisDatabase(dbName)) {
                databasesList.add(dbName);
            }
        }
        dbNamesCursor.close();
        return databasesList;
    }

    private static void iterateContainers(String dbName, MongoDatabase db, boolean scanDocuments) {
        MongoIterable<String> iterable = db.listCollectionNames();
        MongoCursor<String> containersCursor = iterable.iterator();
        while (containersCursor.hasNext()) {
            String cName = containersCursor.next();
            scanResults.addContainer(dbName, cName);
            DocumentStructCollection dsc = new DocumentStructCollection();
            if (scanDocuments) {
                // See https://stackoverflow.com/questions/34545555/how-to-get-size-in-bytes-of-bson-documents
                MongoCollection<RawBsonDocument> mongoCollection = db.getCollection(cName, RawBsonDocument.class);
                MongoCursor<RawBsonDocument> cursor = mongoCollection.find().iterator();
                int documentCount = 0;
                int largestSize = 0;
                RawBsonDocument largestRawDocument = null;
                boolean exceptions = false;
                long estimatedDocumentCount = mongoCollection.estimatedDocumentCount();

                while (cursor.hasNext()) {
                    try {
                        RawBsonDocument rawDoc = cursor.next();
                        int size = rawDoc.getByteBuffer().remaining();
                        if (size > largestSize) {
                            largestRawDocument = rawDoc;
                            largestSize = size;
                        }
                        documentCount++;

                        if (identifyTopTenLargest) {
                            DocumentStruct ds = new DocumentStruct(size, rawDoc);
                            dsc.add(ds);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        exceptions = true;
                    }
                }
                scanResults.updateContainer(dbName, cName, "dbName", dbName);
                scanResults.updateContainer(dbName, cName, "cName", cName);
                scanResults.updateContainer(dbName, cName, "iteratedDocumentCount", documentCount);
                scanResults.updateContainer(dbName, cName, "estimatedDocumentCount", estimatedDocumentCount);
                scanResults.updateContainer(dbName, cName, "largestSize", largestSize);
                scanResults.updateContainer(dbName, cName, "exceptions", exceptions);
                if (largestRawDocument != null) {
                    logLargest(dbName, cName, largestSize, largestRawDocument);
                }
                if (identifyTopTenLargest) {
                    scanResults.updateContainer(dbName, cName, "largestDocumentSizes", dsc.getSizesArray());
                    scanResults.updateContainer(dbName, cName, "largestDocumentPrefixes", dsc.getJsonPrefixesArray());
                }
            }
        }
    }

    private static void logLargest(
            String dbName, String cName, int size, RawBsonDocument largestRawDocument) {
        String json = largestRawDocument.toJson();
        StringBuffer sb = new StringBuffer();
        sb.append("largest doc in db: ");
        sb.append(dbName);
        sb.append(" container: ");
        sb.append(cName);
        sb.append(" size: ");
        sb.append(size);
        sb.append(" doc: ");
        sb.append(json);
        log(sb.toString());

        if (json.length() > JSON_PREFIX_LENGTH) {
            scanResults.updateContainer(
                    dbName,
                    cName,
                    "largestDocJsonPrefix",
                    json.substring(0, JSON_PREFIX_LENGTH));
        }
    }
    private static boolean includeThisDatabase(String dbName) {
        if (dbName == null) {
            return false;
        }
        if (dbName.equalsIgnoreCase("admin")) {
            return false;
        }
        if (dbName.equalsIgnoreCase("config")) {
            return false;
        }
        if (dbName.equalsIgnoreCase("local")) {
            return false;
        }
        return true;
    }

    private static void writeResultsJson() throws Exception {
        String outfile  = scanResults.calculateFilename();
        writeJson(scanResults, outfile, true, true);
    }

    private static void writeJson(Object obj, String outfile, boolean pretty, boolean verbose) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        if (pretty) {
            json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
            writeTextFile(outfile, json, verbose);
        }
        else {
            json = mapper.writeValueAsString(obj);
            writeTextFile(outfile, json, verbose);
            if (verbose) {
                log("file written: " + outfile);
            }
        }
    }

    private static void writeTextFile(String outfile, String text, boolean verbose) throws Exception {
        FileWriter fw = null;
        try {
            fw = new FileWriter(outfile);
            fw.write(text);
            if (verbose) {
                log("file written: " + outfile);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        finally {
            if (fw != null) {
                fw.close();
            }
        }
    }

    public static void log(String msg) {
        System.out.println(msg);
    }
}
