package com.indiabulls.shortagedelivery.notification.helper;

import com.indiabulls.shortagedelivery.notification.dto.ShortageEmailTemplateData;
import com.indiabulls.shortagedelivery.notification.dto.ShortageSMSTemplateData;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class NotificationSender {

    private final NotificationTransportService transport;

    @Value("${shortage.routing.email}") private String emailRoute;
    @Value("${shortage.routing.sms}")   private String smsRoute;

    @Value("${shortage.templates.email.name}")   private String emailTemplate;
    @Value("${shortage.templates.email.sender}") private String emailSender;
    @Value("${shortage.templates.email.subject}") private String emailSubject;

    @Value("${shortage.templates.sms.name}")     private String smsTemplate;
    @Value("${shortage.templates.sms.sender}")   private String smsSender;

    public void sendEmail(List<String> receivers, ShortageEmailTemplateData data) {
        var req = NotificationMessageRequest.<ShortageEmailTemplateData>builder()
                .receivers(receivers)
                .sender(emailSender)
                .subject(emailSubject)
                .templateName(emailTemplate)
                .templateDataJson(data)
                .dataFields(NotificationSpecificFields.builder()
                        .emailDisplayName("Security Shortage")
                        .build())
                .build();
        transport.send(emailRoute, req);
    }

    public void sendSms(List<String> receivers, ShortageSMSTemplateData data) {
        var req = NotificationMessageRequest.<ShortageSMSTemplateData>builder()
                .receivers(receivers)
                .sender(smsSender)
                .subject(null)
                .templateName(smsTemplate)
                .templateDataJson(data)
                .build();
        transport.send(smsRoute, req);
    }
}
