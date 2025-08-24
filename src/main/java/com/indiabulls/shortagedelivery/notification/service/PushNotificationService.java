package com.indiabulls.shortagedelivery.notification.service;

import com.indiabulls.shortagedelivery.notification.helper.NotificationMessageRequest;
import com.indiabulls.shortagedelivery.notification.helper.NotificationSpecificFields;
import com.indiabulls.shortagedelivery.notification.helper.NotificationTransportService;
import com.indiabulls.shortagedelivery.notification.dto.PushNotificationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PushNotificationService {

    private final NotificationTransportService transport;

    @Value("${shortage.templates.push.name}")
    private String pushRoute;



//    @Value("${shortage.templates.push.name}")
//    private String pushTemplate;

    public void sendPush(PushNotificationRequest request) {

        var req = NotificationMessageRequest.<Object>builder()
                .receivers(request.getReceivers())
                .templateName(request.getTemplateName())
                .templateDataJson(request.getTemplateDataJson())
                .dataFields(NotificationSpecificFields.builder()
                        .emailDisplayName(null)
                        .attachments(null)
                        .eventType("event")
                        .build())
                .build();


        log.info("Publishing push notification request: {}", req);

        transport.send(pushRoute, req);
    }
}
