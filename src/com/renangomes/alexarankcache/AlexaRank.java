/**
 * ALEXA RANK CACHE SYSTEM
 *
 * We use the Alexa Rank Top 1M file in order to build a cache in memory.
 *
 * @author Renan Gomes Barreto
 * @version 1.0
 * @since 17-06-2015
 */
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
 * AlexaRank class that will generate (on class load) a rank with top 1M URLs on
 * Alexa.
 *
 * NOTE: This class will download and index the alexa top 1M rank once it is
 * loaded. This means that it can add a few seconds delay on the software load
 * time.
 *
 * @author Renan Gomes Barreto
 * @version 1.0
 * @since 17-06-2015
 */
public class AlexaRank {

    /**
     * The singleton instace for the Alexa Rank Obj
     */
    private static final AlexaRank instance = new AlexaRank();

    /**
     * A Linked Hash Map with the data indexed by Rank.
     */
    private static Map<Integer, String> rankTable;

    /**
     * A Linked Hash Map with the data indexed by Domain.
     */
    private static Map<String, Integer> domainTable;

    /**
     *
     */
    private AlexaRank() {
        rankTable = Collections.synchronizedMap(new LinkedHashMap<Integer, String>(100));
        domainTable = Collections.synchronizedMap(new LinkedHashMap<String, Integer>(100));
        try {
            downloadAlexaTop1M();
        } catch (IOException ex) {
            Logger.getLogger(AlexaRank.class.getName()).log(Level.SEVERE, "Error Reading the Alexa Rank CSV file. All Requests to this class will return null.", ex);
        }
    }

    /**
     * Get A singleton object from this class
     *
     * @return A object of the class AlexaRank
     */
    public static AlexaRank getInstance() {
        return instance;
    }

    /**
     * Download, unzip and index the CSV files inside the alexa Rank top 1M. It
     * keeps a cache on the folder "cache". The cache will be saved for 15 days
     * before re-download.
     *
     * @throws IOException When the zip files can not be read or can not be
     * saved/indexed.
     * @throws MalformedURLException If the Alexa Top 1M zip file have an
     * invalid URL
     */
    private void downloadAlexaTop1M() throws IOException, MalformedURLException {
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
    }

    /**
     * Download a URL and saves into a file
     *
     * @param url The source URL
     * @param outputFile The output file
     * @throws IOException If is not possible to download the file or write the
     * output file. If an Exception is thrown, the outputFile is deleted.
     */
    private void saveUrl(final URL url, final File outputFile) throws IOException {
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

    /**
     * Read the Rank for the domain name provided. NOTE: Before the search is
     * done, this function will try to clear the URL in order to extract only a
     * valid domain name.
     *
     * @param domain The string with a domain name or a partial URL
     * @return The Rank position. NULL if the domain is not found.
     */
    public Integer getRank(String domain) {

        if (domain == null || domain.trim().isEmpty()) {
            return null;
        }
        //Simple Cleanup
        String[] sp = domain.trim().toLowerCase().replaceAll("www.", "").replaceAll("http://", "").replaceAll("https://", "").split("/", 2);
        return domainTable.get(sp[0]);
    }

    /**
     * Read the Domain for the rank provided.
     *
     * @param rank The integer with the position
     * @return The domain that is on the position provided. NULL if the position
     * is not available.
     */
    public String getDomain(Integer rank) {
        return rankTable.get(rank);
    }

    /**
     * This is a sample usage of this class. It try to check some Domains and
     * few rank positions.
     *
     *
     * @param args Comand line arguments
     */
    public static void main(String[] args) {

        //Check some domains names and some partial URLs.
        String[] domainList = {"google.com", "georanker.com", "HTTP://georanker.com", "HTTP://WWW.GeoRanker.com/contactus", "www.georanker.com",
            "www.georanker.com/plans", "https://www.georanker.com/", "alexa.com", "doesnotexists123456.com.au", "", "    ", null};
        System.out.println("Get Rank by URL:");
        for (String domain : domainList) {
            System.out.println(" - Alexa Rank for domain '" + domain + "': " + AlexaRank.getInstance().getRank(domain));
        }

        //Output the top 20 domains
        System.out.println("Get Domain by Rank:");
        for (int rank = 1; rank < 20; rank++) {
            System.out.println(" - " + rank + " : " + AlexaRank.getInstance().getDomain(rank));
        }
    }
}
