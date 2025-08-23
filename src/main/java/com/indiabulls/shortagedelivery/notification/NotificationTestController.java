package com.indiabulls.shortagedelivery.notification;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/notification")
@RequiredArgsConstructor
public class NotificationTestController {

    private final ShortageNotificationService shortageNotificationService;

    @PostMapping("/email")
    public String sendEmailNotification(
            @RequestBody NotificationMessageRequest<ShortageEmailTemplateData> request) {
        shortageNotificationService.sendEmailNotification(request);
        return "Email notification sent";
    }


}


