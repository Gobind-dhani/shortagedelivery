package com.indiabulls.shortagedelivery.ftp;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
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

    @Value("${target.table}") private String targetTable;

    public String loadEquityFile() {
        FTPClient ftpClient = new FTPClient();

        try {
            //  Build today's dynamic path
            LocalDate today = LocalDate.now();
            String dateFolder = today.format(DateTimeFormatter.ofPattern("dd-MMMM-yyyy"));
            // e.g. 21-August-2025

            String todayBasePath = ftpBasePath + "/" + dateFolder + "/stocks";

            // 1) Connect to FTP
            ftpClient.connect(ftpServer);
            ftpClient.login(ftpUser, ftpPass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // 2) Find auction file
            String auctionFilePath = null;
            for (FTPFile file : ftpClient.listFiles(todayBasePath)) {
                String name = file.getName();
                if (name.contains("Equity") && (name.endsWith(".csv.gz") || name.endsWith(".csv"))) {
                    auctionFilePath = todayBasePath + "/" + name;
                    break;
                }
            }
            if (auctionFilePath == null) {
                return " Auction file not found in FTP folder " + todayBasePath;
            }

            // 3) Download file into memory
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean ok = ftpClient.retrieveFile(auctionFilePath, baos);
            if (!ok) {
                return " Failed to download: " + auctionFilePath;
            }

            // 4) Prepare reader (unzip if needed)
            InputStream rawIn = new ByteArrayInputStream(baos.toByteArray());
            InputStream csvIn = auctionFilePath.endsWith(".gz") ? new GZIPInputStream(rawIn) : rawIn;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvIn, StandardCharsets.UTF_8));
                 Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {

                // 5) Read header line
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    return " Empty CSV file";
                }

                String[] headers = headerLine.split(",", -1);
                Map<String, Integer> headerMap = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    headerMap.put(headers[i].trim().toLowerCase(), i);
                }

                if (!headerMap.containsKey("isin") ||
                        !headerMap.containsKey("clntid") ||
                        !headerMap.containsKey("qtyorshrtqty")) {
                    return " Required headers missing. Found: " + headerMap.keySet();
                }

                // 6) Insert into DB
                String insertQuery = "INSERT INTO focus.shortage_delivery " +
                        "(isin, client_id, short_quantity, created_date, updated_date) " +
                        "VALUES (?, ?, ?, now(), now())";

                try (PreparedStatement ps = conn.prepareStatement(insertQuery)) {
                    conn.setAutoCommit(false);

                    int batch = 0;
                    final int BATCH_SIZE = 1000;

                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;

                        String[] cols = line.split(",", -1);

                        String isin = getColumn(cols, headerMap, "isin");
                        String clientId = getColumn(cols, headerMap, "clntid");
                        String qtyStr = getColumn(cols, headerMap, "qtyorshrtqty");

                        Integer shortQty = parseInt(qtyStr);

                        setNullableString(ps, 1, isin);
                        setNullableString(ps, 2, clientId);
                        if (shortQty != null) {
                            ps.setInt(3, shortQty);
                        } else {
                            ps.setNull(3, Types.INTEGER);
                        }

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

                // 7) Update total_quantity from trxn_table_class
                updateTotalQuantityFromTrxn(conn);
            }

            // 8) Cleanup FTP
            if (ftpClient.isConnected()) {
                try { ftpClient.logout(); } catch (Exception ignored) {}
                try { ftpClient.disconnect(); } catch (Exception ignored) {}
            }

            return "✅ Loaded Auction CSV and updated total_quantity in " + targetTable + " from " + todayBasePath;

        } catch (Exception e) {
            e.printStackTrace();
            if (ftpClient.isConnected()) {
                try { ftpClient.logout(); } catch (Exception ignored) {}
                try { ftpClient.disconnect(); } catch (Exception ignored) {}
            }
            return "❌ Error: " + e.getMessage();
        }
    }

    private void updateTotalQuantityFromTrxn(Connection conn) throws Exception {
        String sql = "UPDATE focus.shortage_delivery sd " +
                "SET total_quantity = t.trn_qty, " +
                "    updated_date = now() " +
                "FROM focus.trxn_table_class t " +
                "WHERE sd.isin = t.isin " +
                "  AND sd.client_id = SUBSTRING(t.party_cd, 2)"; // drop leading 'C'

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int rows = ps.executeUpdate();
            conn.commit();
            System.out.println("Updated total_quantity for " + rows + " rows in shortage_delivery");
        }
    }

    private static String getColumn(String[] cols, Map<String, Integer> headerMap, String key) {
        Integer idx = headerMap.get(key);
        if (idx == null || idx >= cols.length) return null;
        return cols[idx].trim();
    }

    private static void setNullableString(PreparedStatement ps, int index, String value) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value.trim());
        }
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
}
