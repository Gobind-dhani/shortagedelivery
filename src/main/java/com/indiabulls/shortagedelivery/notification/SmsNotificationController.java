package com.indiabulls.shortagedelivery.notification;

import com.indiabulls.shortagedelivery.notification.dto.SmsNotificationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class SmsNotificationController {

    private final SmsNotificationService smsNotificationService;

    @PostMapping("/sms")
    public ResponseEntity<String> sendSms(@RequestBody SmsNotificationRequest request) {
        smsNotificationService.sendSms(request);
        return ResponseEntity.ok("SMS notification sent successfully!");
    }
}
