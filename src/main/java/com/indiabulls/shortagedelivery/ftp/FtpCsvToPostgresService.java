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
            LocalDate today = LocalDate.now();
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
}
