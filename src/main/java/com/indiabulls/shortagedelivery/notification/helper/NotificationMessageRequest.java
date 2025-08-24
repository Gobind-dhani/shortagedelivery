package com.indiabulls.shortagedelivery.notification.helper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NotificationMessageRequest<T> {
    private List<String> receivers;
    private String sender;
    private String subject;
    private String templateName;
    private T templateDataJson;
    private NotificationSpecificFields dataFields;
}
