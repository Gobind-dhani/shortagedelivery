package com.indiabulls.shortagedelivery.ftp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class FtpCsvController {

    @Autowired
    private FtpCsvToPostgresService ftpCsvToPostgresService;

    @GetMapping("/fetch")
    public String loadShortFile() {
        return ftpCsvToPostgresService.loadShrtFileAndCompare();
    }

    @GetMapping("/equity")
    public String loadEquityT1File() {
        return ftpCsvToPostgresService.loadDeliveryDpoFile();
    }

    @GetMapping("/shortage-contacts")
    public List<Map<String, Object>> getShortageContacts() {
        return ftpCsvToPostgresService.findClientsWithShortageContacts();
    }

    @GetMapping("/notify-shortages")
    public String notifyShortages() {
        ftpCsvToPostgresService.notifyClientsWithShortages();
        return "Shortage notifications sent successfully!";
    }
    @GetMapping("/auction")
    public String loadAuctionFile() {
        return ftpCsvToPostgresService.loadAuctionFileAndNotify();
    }
}
