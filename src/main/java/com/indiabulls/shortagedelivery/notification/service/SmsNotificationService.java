package com.indiabulls.shortagedelivery.notification.service;

import com.indiabulls.shortagedelivery.notification.helper.NotificationMessageRequest;
import com.indiabulls.shortagedelivery.notification.helper.NotificationSpecificFields;
import com.indiabulls.shortagedelivery.notification.helper.NotificationTransportService;
import com.indiabulls.shortagedelivery.notification.dto.SmsNotificationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class SmsNotificationService {

    private final NotificationTransportService transport;

    @Value("${shortage.templates.sms.name}")
    private String smsRoute;

    public void sendSms(SmsNotificationRequest request) {

        var req = NotificationMessageRequest.<Object>builder()
                .receivers(request.getReceivers())
                .sender("9319409279") // not required for SMS unless your infra demands
                .subject("Shortage_Alert") // irrelevant for SMS
                .templateName(request.getTemplateName())
                .templateDataJson(request.getTemplateDataJson())
                .dataFields(NotificationSpecificFields.builder()
                        .eventType("event") // fixed for SMS as well
                        .build())
                .build();

        log.info("Publishing SMS notification request: {}", req);

        transport.send(smsRoute, req);
    }
}
