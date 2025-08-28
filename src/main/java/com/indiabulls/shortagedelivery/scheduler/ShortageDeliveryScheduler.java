package com.indiabulls.shortagedelivery.scheduler;

import com.indiabulls.shortagedelivery.ftp.FtpCsvToPostgresService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ShortageDeliveryScheduler {

    private final FtpCsvToPostgresService ftpCsvToPostgresService;

    @Value("${shortagedelivery.scheduler.cron}")
    private String cronExpression;

    public ShortageDeliveryScheduler(FtpCsvToPostgresService ftpCsvToPostgresService) {
        this.ftpCsvToPostgresService = ftpCsvToPostgresService;
    }

    /**
     * Runs according to cron expression from application.properties
     */
    @Scheduled(cron = "${shortagedelivery.scheduler.cron}")
    public void runScheduledJobs() {
        System.out.println("⏳ Running FTP shortage jobs...");

        // Step 1: Load Delivery DPO file
        String dpoResult = ftpCsvToPostgresService.loadDeliveryDpoFile();
        System.out.println(dpoResult);

        // Step 2: Compare SHRT file
        String shrtResult = ftpCsvToPostgresService.loadShrtFileAndCompare();
        System.out.println(shrtResult);

        // Step 3: Notify shortage clients
        ftpCsvToPostgresService.notifyClientsWithShortages();

        // Step 4: Auction settlement notifications
        String auctionResult = ftpCsvToPostgresService.loadAuctionFileAndNotify();
        System.out.println(auctionResult);
    }
}
