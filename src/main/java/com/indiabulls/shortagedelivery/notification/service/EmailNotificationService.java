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

        System.out.println("Email notification sent to " + req.getTemplateDataJson().getSYMBOL());
    }



}
