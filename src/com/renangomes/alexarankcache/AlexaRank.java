package com.renangomes.alexarankcache;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 * @author Renan Gomes Barreto
 */
public class AlexaRank {

    private static final AlexaRank instance = new AlexaRank();

    private static Map<Integer, String> rankTable;

    private static Map<String, Integer> domainTable;

    private AlexaRank() {
        rankTable = Collections.synchronizedMap(new LinkedHashMap<Integer, String>(100));
        domainTable = Collections.synchronizedMap(new LinkedHashMap<String, Integer>(100));
        try {
            downloadAlexaTop1M();
        } catch (IOException ex) {
            Logger.getLogger(AlexaRank.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // Runtime initialization
    // By defualt ThreadSafe
    public static AlexaRank getInstance() {
        return instance;
    }

    private LinkedHashMap<Integer, String> downloadAlexaTop1M() throws IOException, MalformedURLException {
        rankTable.clear();
        domainTable.clear();

        File cacheFolder = new File("cache");
        cacheFolder.mkdirs();
        File zipFile = new File(cacheFolder, "top-1m.csv.zip");
        URL downloadURL = new URL("http://s3.amazonaws.com/alexa-static/top-1m.csv.zip");
        int cacheDays = 15;
        if (!zipFile.exists() || !zipFile.canRead() || zipFile.lastModified() < (System.currentTimeMillis() - cacheDays * 24 * 60 * 60 * 1000)) {
            saveUrl(downloadURL, zipFile);
        }

        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipFile)))) {
            ZipEntry entry = zipIn.getNextEntry();

            String csvFile = null;
            if (entry != null) {
                byte[] buffer = new byte[2048];
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                int len = 0;
                while ((len = zipIn.read(buffer)) > 0) {
                    output.write(buffer, 0, len);
                }
                csvFile = new String(output.toByteArray(), "UTF-8");
            }
            if (csvFile != null && !csvFile.isEmpty()) {
                String[] lines = csvFile.split("\n");
                for (String line : lines) {
                    String[] cols = line.split(",", 2);
                    if (cols.length == 2) {
                        Integer rank = new Integer(cols[0]);
                        String domain = cols[1].trim().toLowerCase();

                        if (rank > 0 && !domain.isEmpty()) {
                            rankTable.put(rank, domain);
                            domainTable.put(domain, rank);
                        }

                    }
                }

            }
        } catch (IOException ioe) {
            zipFile.delete();
            throw ioe;
        }
        return null;
    }

    private void saveUrl(final URL url, final File outputFile) throws MalformedURLException, IOException {
        if (url == null || outputFile == null) {
            throw new IllegalArgumentException("Illegal Argument. Any of the arguments can NOT be null.");
        }
        BufferedInputStream in = null;
        FileOutputStream fout = null;
        try {
            in = new BufferedInputStream(url.openStream());
            fout = new FileOutputStream(outputFile);

            final byte data[] = new byte[1024];
            int count;
            while ((count = in.read(data, 0, 1024)) != -1) {
                fout.write(data, 0, count);
            }
        } catch (IOException ioe) {
            outputFile.delete();
            throw ioe;
        } finally {
            if (in != null) {
                in.close();
            }
            if (fout != null) {
                fout.close();
            }
        }
    }

    public Integer getRank(String domain) {

        if (domain == null || domain.trim().isEmpty()) {
            return null;
        }
        return domainTable.get(domain);
    }

    public String getDomain(Integer rank) {
        return rankTable.get(rank);
    }

    public static void main(String[] args) {

        String[] domainList = {"google.com", "georanker.com", "www.georanker.com", "www.georanker.com/plans",
            "https://www.georanker.com/", "alexa.com", "doesnotexists123456.com.au",};

        for (String domain : domainList) {
            System.out.println("Alexa Rank for domain '" + domain + "': " + AlexaRank.getInstance().getRank(domain));
        }

        for (int rank = 1; rank < 100; rank++) { 
            System.out.println(rank + " -> " + AlexaRank.getInstance().getDomain(rank));
        }
    }
}
