package com.indiabulls.shortagedelivery.notification.controller;

import com.indiabulls.shortagedelivery.notification.service.PushNotificationService;
import com.indiabulls.shortagedelivery.notification.dto.PushNotificationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class PushNotificationController {

    private final PushNotificationService pushNotificationService;

    @PostMapping("/push")
    public ResponseEntity<String> sendPush(@RequestBody PushNotificationRequest request) {
        pushNotificationService.sendPush(request);
        return ResponseEntity.ok("Push notification sent successfully!");
    }
}
