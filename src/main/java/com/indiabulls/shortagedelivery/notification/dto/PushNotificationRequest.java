package com.indiabulls.shortagedelivery.notification.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class PushNotificationRequest {
    private List<String> receivers;       // e.g. clientIds
    private String templateName;          // template identifier
    private Map<String, Object> templateDataJson; // template data
    private String eventType;             // custom event type
}
