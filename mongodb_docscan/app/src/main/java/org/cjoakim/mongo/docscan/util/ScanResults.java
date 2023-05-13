package org.cjoakim.mongo.docscan.util;

import com.mongodb.ConnectionString;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

/**
 * An instance of this class is used in the main App class, to both collect the scan results
 * as well as render them as JSON to a 'DocScanResults_<host>_<epoch>.json' file.
 *
 * Microsoft CosmosDB GBB team: Chris Joakim, Aleksey Savateyev, Emmanuel Deletang, et al.
 */
public class ScanResults {

    // Instance variables:

    public String version;
    private long startEpoch;
    private String startDate;
    private long finishEpoch;
    private long elapsedMs;
    private String clusterName;
    private String username;
    private int totalDocumentCount;
    private boolean connected;
    private boolean exceptionEncountered;
    private List<String> hosts;
    private List<String> databases;
    private HashMap<String, HashMap<String, Object>> containerInfo = new HashMap<String, HashMap<String, Object>>();

    public ScanResults() {
        super();
        try {
            version = "2023/04/03 10:38am";
            startEpoch = System.currentTimeMillis();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:MM z");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            startDate = sdf.format(new Date(startEpoch));
            username  = System.getProperty("user.name");
        }
        catch (Exception e) {
            username = "unknown";
        }
    }

    public void setSourceUrl(String connString) {
        ConnectionString cs = new ConnectionString(connString);
        hosts = cs.getHosts();
    }

    public String getVersion() {
        return version;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public List<String> getDatabases() {
        return databases;
    }

    public void setDatabases(List<String> databases) {
        this.databases = databases;
    }

    public String calculateFilename() throws Exception {
        String hostPart = String.join("_", getHosts());
        String timePart = "" + getStartEpoch();
        return "out/DocScanResults_" + clusterName + "_" + hostPart + "_" + timePart +  ".json";
    }

    public long getStartEpoch() {
        return startEpoch;
    }

    public void setStartEpoch(long startEpoch) {
        this.startEpoch = startEpoch;
    }

    public String getStartDate() {
        return startDate;
    }

    public long getFinishEpoch() {
        return finishEpoch;
    }

    public long getElapsedMs() {
        return elapsedMs;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getUsername() {
        return username;
    }

    public void finish() {
        this.finishEpoch = System.currentTimeMillis();
        this.elapsedMs = this.finishEpoch - this.startEpoch;
    }

    public HashMap<String, HashMap<String, Object>> getContainerInfo() {
        return containerInfo;
    }

    public int getTotalDocumentCount() {
        return totalDocumentCount;
    }

    public boolean isExceptionEncountered() {
        return exceptionEncountered;
    }

    public void setExceptionEncountered() {
        this.exceptionEncountered = true;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public void addContainer(String dbName, String cName) {
        if (dbName == null) {
            return;
        }
        if (cName == null) {
            return;
        }

        String key = "" + dbName + "|" + cName;

        if (!this.containerInfo.containsKey(key)) {
            HashMap<String, Object> hash = new HashMap<String, Object>();
            containerInfo.put(key, hash);
        }
    }

    public void updateContainer(String dbName, String cName, String metric, Object value) {
        if (dbName == null) {
            return;
        }
        if (cName == null) {
            return;
        }
        if (metric == null) {
            return;
        }
        if (value == null) {
            return;
        }

        String key = "" + dbName + "|" + cName;

        if (this.containerInfo.containsKey(key)) {
            HashMap<String, Object> values = this.containerInfo.get(key);
            values.put(metric, value);
        }

        if (metric.equalsIgnoreCase("documentCount")) {
            this.totalDocumentCount = this.totalDocumentCount + (int) value;
        }
    }

    public static void log(String msg) {
        System.out.println(msg);
    }
}
