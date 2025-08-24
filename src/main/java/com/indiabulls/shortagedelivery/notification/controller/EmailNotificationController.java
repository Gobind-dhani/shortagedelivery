package com.indiabulls.shortagedelivery.notification.controller;

import com.indiabulls.shortagedelivery.notification.helper.NotificationMessageRequest;
import com.indiabulls.shortagedelivery.notification.dto.ShortageEmailTemplateData;
import com.indiabulls.shortagedelivery.notification.service.EmailNotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class EmailNotificationController {

    private final EmailNotificationService shortageNotificationService;

    @PostMapping("/email")
    public String sendEmailNotification(
            @RequestBody NotificationMessageRequest<ShortageEmailTemplateData> request) {
        shortageNotificationService.sendEmailNotification(request);
        return "Email notification sent";
    }


}


