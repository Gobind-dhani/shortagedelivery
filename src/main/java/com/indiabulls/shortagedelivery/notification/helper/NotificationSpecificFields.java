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
public class NotificationSpecificFields {
    private String emailDisplayName;
    private List<Object> attachments;
    private String eventType;
}
