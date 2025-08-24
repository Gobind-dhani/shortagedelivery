package com.indiabulls.shortagedelivery.notification.service;

import com.indiabulls.shortagedelivery.notification.helper.NotificationMessageRequest;
import com.indiabulls.shortagedelivery.notification.helper.NotificationSender;
import com.indiabulls.shortagedelivery.notification.dto.ShortageEmailTemplateData;
import com.indiabulls.shortagedelivery.notification.dto.ShortageSMSTemplateData;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;

@Service
@RequiredArgsConstructor
public class EmailNotificationService {

    private final NotificationSender notificationSender;

    @Value("${spring.datasource.url}")
    private String jdbcUrl;

    @Value("${spring.datasource.username}")
    private String jdbcUser;

    @Value("${spring.datasource.password}")
    private String jdbcPass;

    @Value("${shortage.routing.email}")
    private String emailRoute;

    @Value("${shortage.templates.email.name}")
    private String emailTemplate;

    @Value("${shortage.templates.email.sender}")
    private String emailSender;

    @Value("${shortage.templates.email.subject}")
    private String emailSubject;

    @Value("${shortage.routing.sms}")
    private String smsRoute;

    @Value("${shortage.templates.sms.name}")
    private String smsTemplate;

    @Value("${shortage.templates.sms.sender}")
    private String smsSender;

    // --- New method to accept JSON payload for Email ---
    public void sendEmailNotification(NotificationMessageRequest<ShortageEmailTemplateData> request) {
        var req = NotificationMessageRequest.<ShortageEmailTemplateData>builder()
                .receivers(request.getReceivers())
                .sender(emailSender)              // override with configured sender
                .subject(emailSubject)            // override with configured subject
                .templateName(emailTemplate)      // override with configured template
                .templateDataJson(request.getTemplateDataJson())
                .dataFields(request.getDataFields())
                .build();

        // Forward to NotificationSender
        notificationSender.sendEmail(req.getReceivers(), req.getTemplateDataJson());

        System.out.println("Email notification sent to " + req.getTemplateDataJson().getCLIENT());
    }


    // --- Existing DB-driven notification flow stays same ---
    public void notifyClientsWithShortage() {
        String sql = "SELECT sd.client_id, sd.isin, sd.short_quantity, sd.average_price, " +
                "       cm.email_no, cm.mobile_no " +
                "FROM focus.shortage_delivery sd " +
                "JOIN focus.cust_mst cm ON cm.client_id = sd.client_id " +
                "WHERE DATE(sd.created_date) = CURRENT_DATE " +
                "  AND sd.short_quantity IS NOT NULL " +
                "  AND sd.short_quantity > 0";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String clientId = rs.getString("client_id");
                String isin = rs.getString("isin");
                int shortQty = rs.getInt("short_quantity");
                Double avgPrice = rs.getObject("average_price") != null ? rs.getDouble("average_price") : null;
                String email = rs.getString("email_no");
                String mobile = rs.getString("mobile_no");

                // --- Build Email ---
                if (email != null && !email.isEmpty()) {
                    ShortageEmailTemplateData emailData = ShortageEmailTemplateData.builder()
                            .CLIENT(clientId)
                            .ISIN(isin)
                            .QTY(shortQty)
                            .PRICE(avgPrice)
                            .build();

                    notificationSender.sendEmail(Collections.singletonList(email), emailData);
                }

                // --- Build SMS ---
                if (mobile != null && !mobile.isEmpty()) {
                    ShortageSMSTemplateData smsData = ShortageSMSTemplateData.builder()
                            .clientId(clientId)
                            .isin(isin)
                            .shortageQty(shortQty)
                            .build();

                    notificationSender.sendSms(Collections.singletonList(mobile), smsData);
                }

                System.out.println("Notified client=" + clientId + " isin=" + isin + " qty=" + shortQty);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to send shortage notifications", e);
        }
    }
}
