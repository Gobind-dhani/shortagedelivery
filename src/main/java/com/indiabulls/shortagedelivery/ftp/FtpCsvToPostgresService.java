package com.indiabulls.shortagedelivery.ftp;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPInputStream;

@Service
public class FtpCsvToPostgresService {

    @Value("${ftp.server}") private String ftpServer;
    @Value("${ftp.user}") private String ftpUser;
    @Value("${ftp.pass}") private String ftpPass;
    @Value("${ftp.base-path}") private String ftpBasePath; // e.g. /indiabulls/ib-automation/backoffice-input

    @Value("${spring.datasource.url}") private String jdbcUrl;
    @Value("${spring.datasource.username}") private String jdbcUser;
    @Value("${spring.datasource.password}") private String jdbcPass;

    /**
     * Fetch the SHRT file from FTP, parse CSV, and dump shortage data into Postgres.
     */
    public String loadShortFile() {
        FTPClient ftpClient = new FTPClient();

        try {
            // Build today's path
            LocalDate today = LocalDate.now().minusDays(1);
            String dateFolder = today.format(DateTimeFormatter.ofPattern("dd-MMMM-yyyy"));
            String todayBasePath = ftpBasePath + "/" + dateFolder + "/stocks";

            // Connect FTP
            ftpClient.connect(ftpServer);
            ftpClient.login(ftpUser, ftpPass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Locate SHRT file
            String shrtFilePath = null;
            for (FTPFile file : ftpClient.listFiles(todayBasePath)) {
                String name = file.getName();
                if (name.matches("NCL_C_08756_SHRT_.*\\.csv\\.gz")) {
                    shrtFilePath = todayBasePath + "/" + name;
                    System.out.println("Found SHRT file: " + shrtFilePath);
                    break;
                }
            }
            if (shrtFilePath == null) {
                return " SHRT file not found in " + todayBasePath;
            }

            // Download into memory
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean ok = ftpClient.retrieveFile(shrtFilePath, baos);
            if (!ok) {
                return " Failed to download: " + shrtFilePath;
            }

            // Prepare CSV reader (gzip → utf-8 text)
            InputStream rawIn = new ByteArrayInputStream(baos.toByteArray());
            InputStream csvIn = new GZIPInputStream(rawIn);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvIn, StandardCharsets.UTF_8));
                 Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {

                // Read header
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    return " Empty SHRT file";
                }

                String[] headers = headerLine.split(",", -1);
                Map<String, Integer> headerMap = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    headerMap.put(headers[i].trim().toLowerCase(), i);
                }

                if (!headerMap.containsKey("security symbol") ||
                        !headerMap.containsKey("short quantity") ||
                        !headerMap.containsKey("settlement no")) {
                    return " Required headers missing. Found: " + headerMap.keySet();
                }

                // Prepare UPSERT statement
                String insertSql = "INSERT INTO focus.short_delivery " +
                        "(security_symbol, short_quantity, settlement_no, created_date, updated_date) " +
                        "VALUES (?, ?, ?, now(), now()) ";

                try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                    conn.setAutoCommit(false);

                    String line;
                    int batch = 0;
                    final int BATCH_SIZE = 500;

                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        String[] cols = line.split(",", -1);

                        String securitySymbol = getColumn(cols, headerMap, "security symbol");
                        String shortQtyStr = getColumn(cols, headerMap, "short quantity");
                        String settlementStr = getColumn(cols, headerMap, "settlement no");

                        Integer shortQty = parseInt(shortQtyStr);
                        Integer settlementNo = parseInt(settlementStr);

                        ps.setString(1, securitySymbol);
                        if (shortQty != null) ps.setInt(2, shortQty); else ps.setNull(2, Types.INTEGER);
                        if (settlementNo != null) ps.setInt(3, settlementNo); else ps.setNull(3, Types.INTEGER);

                        ps.addBatch();
                        batch++;

                        if (batch % BATCH_SIZE == 0) {
                            ps.executeBatch();
                            conn.commit();
                        }
                    }

                    ps.executeBatch();
                    conn.commit();
                }
            }

            // Disconnect FTP
            ftpClient.logout();
            ftpClient.disconnect();

            return " Loaded SHRT file into focus.short_delivery";

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (Exception ignored) {}
            return " Error: " + e.getMessage();
        }
    }

    // helpers
    private static String getColumn(String[] cols, Map<String, Integer> headerMap, String key) {
        Integer idx = headerMap.get(key);
        if (idx == null || idx >= cols.length) return null;
        return cols[idx].trim();
    }

    private static Integer parseInt(String val) {
        try {
            if (val == null) return null;
            String t = val.trim();
            if (t.isEmpty()) return null;
            t = t.replace(",", "");
            return Integer.parseInt(t);
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * Fetch the DeliveryDPO file from FTP, parse it, and update Postgres with qty_received_t1, isin, clnt_id.
     */

    /**
     * Fetch the DeliveryDPO file from FTP, parse it, and update Postgres with qty_received_t1, isin, clnt_id.
     */
    public String loadDeliveryDpoFile() {
        FTPClient ftpClient = new FTPClient();

        try {
            // Build today's path
            LocalDate today = LocalDate.now().minusDays(1);
            String dateFolder = today.format(DateTimeFormatter.ofPattern("dd-MMMM-yyyy"));
            String todayBasePath = ftpBasePath + "/" + dateFolder + "/stocks";

            // Connect FTP
            ftpClient.connect(ftpServer);
            ftpClient.login(ftpUser, ftpPass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Locate DeliveryDPO file
            String dpoFilePath = null;
            for (FTPFile file : ftpClient.listFiles(todayBasePath)) {
                String name = file.getName();
                if (name.matches("DeliveryDpo_NCL_CM_EquityT1_CM_08756_.*\\.csv\\.gz")) {
                    dpoFilePath = todayBasePath + "/" + name;
                    System.out.println("Found DeliveryDPO file: " + dpoFilePath);
                    break;
                }
            }
            if (dpoFilePath == null) {
                return " DeliveryDPO file not found in " + todayBasePath;
            }

            // Download into memory
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean ok = ftpClient.retrieveFile(dpoFilePath, baos);
            if (!ok) {
                return " Failed to download: " + dpoFilePath;
            }

            // Prepare CSV reader (gzip → utf-8 text)
            InputStream rawIn = new ByteArrayInputStream(baos.toByteArray());
            InputStream csvIn = new GZIPInputStream(rawIn);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvIn, StandardCharsets.UTF_8));
                 Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {

                // Read header
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    return " Empty DeliveryDPO file";
                }

                String[] headers = headerLine.split(",", -1);
                Map<String, Integer> headerMap = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    headerMap.put(headers[i].trim().toLowerCase(), i);
                }

                if (!headerMap.containsKey("tckrsymb") ||
                        !headerMap.containsKey("sctiessttlmtxid") ||
                        !headerMap.containsKey("qtyorshrtqty") ||
                        !headerMap.containsKey("isin") ||
                        !headerMap.containsKey("clntid")) {
                    return " Required headers missing in DeliveryDPO. Found: " + headerMap.keySet();
                }


                // Prepare UPDATE statement
                String updateSql = "UPDATE focus.short_delivery " +
                        "SET qty_received_t1 = ?, isin = ?, clnt_id = ?, updated_date = now() " +
                        "WHERE security_symbol = ? AND settlement_no = ?";

                try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                    conn.setAutoCommit(false);

                    String line;
                    int batch = 0;
                    final int BATCH_SIZE = 500;

                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        String[] cols = line.split(",", -1);

                        String securitySymbol = getColumn(cols, headerMap, "tckrsymb");
                        String settlementStr = getColumn(cols, headerMap, "sctiessttlmtxid");
                        String qtyStr = getColumn(cols, headerMap, "qtyorshrtqty");
                        String isin = getColumn(cols, headerMap, "isin");
                        String clntId = getColumn(cols, headerMap, "clntid");

//                        Integer settlementNo = parseInt(settlementStr);
                        Integer qty = parseInt(qtyStr);

                        if (securitySymbol == null || settlementStr == null) {
                            continue; // skip invalid row
                        }

                         ps.setInt(1, qty);
                        ps.setString(2, isin);
                        ps.setString(3, clntId);
                        ps.setString(4, securitySymbol);
                        ps.setString(5, settlementStr);

                        ps.addBatch();
                        batch++;

                        if (batch % BATCH_SIZE == 0) {
                            ps.executeBatch();
                            conn.commit();
                        }
                    }

                    ps.executeBatch();
                    conn.commit();
                }
            }

            // Disconnect FTP
            ftpClient.logout();
            ftpClient.disconnect();

            return " Updated focus.short_delivery with DeliveryDPO data";

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (Exception ignored) {}
            return " Error: " + e.getMessage();
        }
    }






//    public String loadDeliveryDpoFile() {
//        FTPClient ftpClient = new FTPClient();
//
//        try {
//            // Build today's path
//            LocalDate today = LocalDate.now().minusDays(1);            String dateFolder = today.format(DateTimeFormatter.ofPattern("dd-MMMM-yyyy"));
//            String todayBasePath = ftpBasePath + "/" + dateFolder + "/stocks";
//
//            // Connect FTP
//            ftpClient.connect(ftpServer);
//            ftpClient.login(ftpUser, ftpPass);
//            ftpClient.enterLocalPassiveMode();
//            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
//
//            // Locate DeliveryDPO file
//            String dpoFilePath = null;
//            for (FTPFile file : ftpClient.listFiles(todayBasePath)) {
//                String name = file.getName();
//                if (name.matches("DeliveryDpo_NCL_CM_EquityT1_CM_08756_.*\\.csv\\.gz")) {
//                    dpoFilePath = todayBasePath + "/" + name;
//                    System.out.println("Found DeliveryDPO file: " + dpoFilePath);
//                    break;
//                }
//            }
//            if (dpoFilePath == null) {
//                return "DeliveryDPO file not found in " + todayBasePath;
//            }
//
//            // Download into memory
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            boolean ok = ftpClient.retrieveFile(dpoFilePath, baos);
//            if (!ok) {
//                return "Failed to download: " + dpoFilePath;
//            }
//
//            // Prepare CSV reader (gzip → utf-8 text)
//            InputStream rawIn = new ByteArrayInputStream(baos.toByteArray());
//            InputStream csvIn = new GZIPInputStream(rawIn);
//
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvIn, StandardCharsets.UTF_8));
//                 Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {
//
//                conn.setAutoCommit(false);
//                String insertSql = "INSERT INTO focus.short_delivery " +
//                        "(settlement_no, clnt_id, qty_received_t1, security_symbol, isin) " +
//                        "VALUES (?, ?, ?, ?, ?)";
//
//                try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
//                    String headerLine = reader.readLine();
//                    if (headerLine == null) {
//                        return "Empty DeliveryDPO file!";
//                    }
//
//                    String[] headers = headerLine.split(",", -1);
//                    Map<String, Integer> headerMap = new HashMap<>();
//                    for (int i = 0; i < headers.length; i++) {
//                        headerMap.put(headers[i].trim(), i);
//                    }
//
//                    // Ensure required headers exist
//                    if (!headerMap.containsKey("SctiesSttlmTxId") ||
//                            !headerMap.containsKey("ClntId") ||
//                            !headerMap.containsKey("QtyORShrtQty") ||
//                            !headerMap.containsKey("TckrSymb") ||
//                            !headerMap.containsKey("ISIN")) {
//                        return "Missing required headers in DeliveryDPO file!";
//                    }
//
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        String[] cols = line.split(",", -1);
//
//                        String settlementNo = cols[headerMap.get("SctiesSttlmTxId")].trim();
//                        String clntId = cols[headerMap.get("ClntId")].trim();
//                        int qty = Integer.parseInt(cols[headerMap.get("QtyORShrtQty")].trim());
//                        String tckrSymb = cols[headerMap.get("TckrSymb")].trim();
//                        String isin = cols[headerMap.get("ISIN")].trim();
//
//                        // Convert settlementNo to long for BIGINT
//
//
//
//                        ps.setString(1, settlementNo);   // BIGINT
//                        ps.setString(2, clntId);       // TEXT
//                        ps.setInt(3, qty);             // INTEGER
//                        ps.setString(4, tckrSymb);     // TEXT
//                        ps.setString(5, isin);         // TEXT
//
//                        ps.addBatch();
//                    }
//                    ps.executeBatch();
//
//                    // Update total_quantity from trxn_table_class
//                    updateShortDeliveryWithTotalQty(conn);
//
//                    conn.commit();
//                }
//            }
//
//            // Disconnect FTP
//            ftpClient.logout();
//            ftpClient.disconnect();
//
//            return "Updated focus.short_delivery with DeliveryDPO data";
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            try {
//                if (ftpClient.isConnected()) {
//                    ftpClient.logout();
//                    ftpClient.disconnect();
//                }
//            } catch (Exception ignored) {}
//            return "Error: " + e.getMessage();
//        }
//    }
//
//    private void updateShortDeliveryWithTotalQty(Connection conn) throws SQLException {
//        String updateSql =
//                "UPDATE focus.short_delivery sd " +
//                        "SET total_quantity = sub.total_trn_qty " +
//                        "FROM (" +
//                        "    SELECT settlement_no, isin, SUBSTRING(party_cd FROM 2) AS clnt_id, SUM(trn_qty) AS total_trn_qty " +
//                        "    FROM focus.trxn_table_class " +
//                        "    GROUP BY settlement_no, isin, SUBSTRING(party_cd FROM 2)" +
//                        ") sub " +
//                        "WHERE sd.settlement_no = sub.settlement_no " +
//                        "AND sd.isin = sub.isin " +
//                        "AND sd.clnt_id = sub.clnt_id";
//
//        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
//            int updated = ps.executeUpdate();
//            System.out.println("Updated total_quantity for " + updated + " rows in short_delivery");
//        }
//    }

}
