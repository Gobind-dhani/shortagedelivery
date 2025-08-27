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
            LocalDate today = LocalDate.now().minusDays(1);            String dateFolder = today.format(DateTimeFormatter.ofPattern("dd-MMMM-yyyy"));
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
                return "DeliveryDPO file not found in " + todayBasePath;
            }

            // Download into memory
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean ok = ftpClient.retrieveFile(dpoFilePath, baos);
            if (!ok) {
                return "Failed to download: " + dpoFilePath;
            }

            // Prepare CSV reader (gzip → utf-8 text)
            InputStream rawIn = new ByteArrayInputStream(baos.toByteArray());
            InputStream csvIn = new GZIPInputStream(rawIn);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvIn, StandardCharsets.UTF_8));
                 Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {

                conn.setAutoCommit(false);
                String insertSql = "INSERT INTO focus.short_delivery " +
                        "(settlement_no, clnt_id, qty_received_t1, security_symbol, isin) " +
                        "VALUES (?, ?, ?, ?, ?)";

                try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                    String headerLine = reader.readLine();
                    if (headerLine == null) {
                        return "Empty DeliveryDPO file!";
                    }

                    String[] headers = headerLine.split(",", -1);
                    Map<String, Integer> headerMap = new HashMap<>();
                    for (int i = 0; i < headers.length; i++) {
                        headerMap.put(headers[i].trim(), i);
                    }

                    // Ensure required headers exist
                    if (!headerMap.containsKey("SctiesSttlmTxId") ||
                            !headerMap.containsKey("ClntId") ||
                            !headerMap.containsKey("QtyORShrtQty") ||
                            !headerMap.containsKey("TckrSymb") ||
                            !headerMap.containsKey("ISIN")) {
                        return "Missing required headers in DeliveryDPO file!";
                    }

                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] cols = line.split(",", -1);

                        String settlementNo = cols[headerMap.get("SctiesSttlmTxId")].trim();
                        String clntId = cols[headerMap.get("ClntId")].trim();
                        int qty = Integer.parseInt(cols[headerMap.get("QtyORShrtQty")].trim());
                        String tckrSymb = cols[headerMap.get("TckrSymb")].trim();
                        String isin = cols[headerMap.get("ISIN")].trim();

                        // Convert settlementNo to long for BIGINT



                        ps.setString(1, settlementNo);
                        ps.setString(2, clntId);       // TEXT
                        ps.setInt(3, qty);             // INTEGER
                        ps.setString(4, tckrSymb);     // TEXT
                        ps.setString(5, isin);         // TEXT

                        ps.addBatch();
                    }
                    ps.executeBatch();

                    // Update total_quantity from trxn_table_class
                    updateShortDeliveryWithTotalQty(conn);

                    conn.commit();
                }
            }

            // Disconnect FTP
            ftpClient.logout();
            ftpClient.disconnect();

            return "Updated focus.short_delivery with DeliveryDPO data";

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (Exception ignored) {}
            return "Error: " + e.getMessage();
        }
    }

    private void updateShortDeliveryWithTotalQty(Connection conn) throws SQLException {
        String updateSql =
                "UPDATE focus.short_delivery sd " +
                        "SET total_quantity = sub.total_trn_qty " +
                        "FROM (" +
                        "    SELECT settlement_no, UPPER(isin) AS isin, TRIM(LEADING 'C' FROM party_cd) AS clnt_id, " +
                        "           SUM(trn_qty) AS total_trn_qty " +
                        "    FROM focus.trxn_table_class " +
                        "    GROUP BY settlement_no, UPPER(isin), TRIM(LEADING 'C' FROM party_cd)" +
                        ") sub " +
                        "WHERE sd.settlement_no = sub.settlement_no " +
                        "AND UPPER(sd.isin) = sub.isin " +
                        "AND sd.clnt_id = sub.clnt_id";

        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            int updated = ps.executeUpdate();
            System.out.println("Updated total_quantity for " + updated + " rows in short_delivery");
        }
    }

    public String loadShrtFileAndCompare() {
        FTPClient ftpClient = new FTPClient();

        try {
            // Build yesterday’s folder path
            LocalDate today = LocalDate.now().minusDays(1);
            String dateFolder = today.format(DateTimeFormatter.ofPattern("dd-MMMM-yyyy"));
            String todayBasePath = ftpBasePath + "/" + dateFolder + "/stocks";

            // Connect FTP
            ftpClient.connect(ftpServer);
            ftpClient.login(ftpUser, ftpPass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Locate SHRT file (pattern may differ, adjust regex if needed)
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
                return "SHRT file not found in " + todayBasePath;
            }

            // Download file into memory
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean ok = ftpClient.retrieveFile(shrtFilePath, baos);
            if (!ok) {
                return "Failed to download: " + shrtFilePath;
            }

            // Prepare reader (gzip → utf-8 text)
            InputStream rawIn = new ByteArrayInputStream(baos.toByteArray());
            InputStream csvIn = new GZIPInputStream(rawIn);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvIn, StandardCharsets.UTF_8));
                 Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {

                String headerLine = reader.readLine();
                if (headerLine == null) {
                    return "Empty SHRT file!";
                }

                String[] headers = headerLine.split(",", -1);
                Map<String, Integer> headerMap = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    headerMap.put(headers[i].trim(), i);
                }

                // Expected headers: adjust according to SHRT file structure
                if (!headerMap.containsKey("Security Symbol")) {
                    return "Missing required header 'Symbol' in SHRT file!";
                }

                // Query (no client id, so we only match on symbol)
                String checkSql = "SELECT clnt_id, qty_received_t1, total_quantity " +
                        "FROM focus.short_delivery " +
                        "WHERE security_symbol = ?";

                List<String> mismatches = new ArrayList<>();

                try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] cols = line.split(",", -1);

                        String symbol = cols[headerMap.get("Security Symbol")].trim();

                        ps.setString(1, symbol);

                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                String clntId = rs.getString("clnt_id");
                                int qtyReceived = rs.getInt("qty_received_t1");
                                int totalQty = rs.getInt("total_quantity");

                                if (qtyReceived != totalQty) {
                                    mismatches.add("Client=" + clntId +
                                            ", Symbol=" + symbol +
                                            ", total_quantity=" + totalQty +
                                            ", qty_received_t1=" + qtyReceived);
                                }
                            }
                        }
                    }
                }

                if (mismatches.isEmpty()) {
                    return "No mismatches found between SHRT and short_delivery";
                } else {
                    mismatches.forEach(System.out::println);
                    return "Found " + mismatches.size() + " mismatches. Check logs for details.";
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (Exception ignored) {}
        }
    }


}
