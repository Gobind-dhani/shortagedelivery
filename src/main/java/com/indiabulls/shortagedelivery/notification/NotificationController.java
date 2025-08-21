package com.indiabulls.shortagedelivery.notification;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationSender notificationSender;

    // ==== Requests ====
    @Data
    public static class EmailRequest {
        private List<String> receivers;
        private ShortageEmailTemplateData data;
    }

    @Data
    public static class SmsRequest {
        private List<String> receivers;
        private ShortageSMSTemplateData data;
    }

    // ==== Endpoints ====
    @PostMapping("/email")
    public String sendEmail(@RequestBody EmailRequest request) {
        notificationSender.sendEmail(request.getReceivers(), request.getData());
        return "Email notification sent";
    }

    @PostMapping("/sms")
    public String sendSms(@RequestBody SmsRequest request) {
        notificationSender.sendSms(request.getReceivers(), request.getData());
        return "SMS notification sent";
    }
}
