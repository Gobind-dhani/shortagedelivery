package com.indiabulls.shortagedelivery.ftp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FtpCsvController {

    @Autowired
    private FtpCsvToPostgresService ftpCsvToPostgresService;

    @GetMapping("/fetch")
    public String loadEquityT1File() {
        return ftpCsvToPostgresService.loadShortFile();
    }
}
