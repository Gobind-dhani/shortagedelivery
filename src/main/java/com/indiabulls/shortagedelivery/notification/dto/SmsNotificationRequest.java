package com.indiabulls.shortagedelivery.notification.dto;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class SmsNotificationRequest {
    private List<String> receivers;          // list of phone numbers
    private String templateName;             // SMS template identifier
    private Map<String, Object> templateDataJson; // SMS template data (placeholders)
}
