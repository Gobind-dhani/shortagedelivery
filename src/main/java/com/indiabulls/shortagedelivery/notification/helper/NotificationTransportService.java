package com.indiabulls.shortagedelivery.notification.helper;

public interface NotificationTransportService {
    boolean send(String routeName, NotificationMessageRequest<?> notificationMessageRequest);
}
