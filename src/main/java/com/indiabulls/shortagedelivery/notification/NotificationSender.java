package com.indiabulls.shortagedelivery.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import javax.annotation.PostConstruct;
import lombok.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
class NotificationMessageRequest<T> {
    private List<String> receivers;
    private String sender;
    private String subject;
    private String templateName;
    private T templateDataJson;
    private NotificationSpecificFields dataFields;
}




@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
class NotificationSpecificFields {
    private String emailDisplayName;
    private List<Object> attachments;
}

interface NotificationTransportService {
    boolean send(String routeName, NotificationMessageRequest<?> notificationMessageRequest);
}

@Component
class RabbitMqTransportService implements NotificationTransportService {

    private Connection connection;
    private Channel channel;

    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.port}")
    private int port;

    @Value("${rabbitmq.username}")
    private String username;

    @Value("${rabbitmq.password}")
    private String password;

    @Value("${rabbitmq.exchange}")
    private String exchange;

    @Value("${rabbitmq.exchangeType}")
    private String exchangeType;

    private final ObjectMapper objectMapper;

    public RabbitMqTransportService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            this.connection = factory.newConnection();
            this.channel = connection.createChannel();

            channel.exchangeDeclare(exchange, exchangeType, true);
            channel.queueDeclare("notification.queue", true, false, false, null);
            channel.queueBind("notification.queue", exchange, "notification.key");

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RabbitMQ connection", e);
        }
    }

    @Override
    public boolean send(String routeName, NotificationMessageRequest<?> notificationMessageRequest) {
        try {
            String message = objectMapper.writeValueAsString(notificationMessageRequest);
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .build();
            channel.basicPublish(exchange, routeName, props, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("Sent to RabbitMQ exchange=" + exchange + " route=" + routeName + " message=" + message);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

@Component
@RequiredArgsConstructor
public class NotificationSender {

    private final NotificationTransportService transport;

    @Value("${shortage.routing.email}") private String emailRoute;
    @Value("${shortage.routing.sms}")   private String smsRoute;

    @Value("${shortage.templates.email.name}")   private String emailTemplate;
    @Value("${shortage.templates.email.sender}") private String emailSender;
    @Value("${shortage.templates.email.subject}") private String emailSubject;

    @Value("${shortage.templates.sms.name}")     private String smsTemplate;
    @Value("${shortage.templates.sms.sender}")   private String smsSender;

    public void sendEmail(List<String> receivers, ShortageEmailTemplateData data) {
        var req = NotificationMessageRequest.<ShortageEmailTemplateData>builder()
                .receivers(receivers)
                .sender(emailSender)
                .subject(emailSubject)
                .templateName(emailTemplate)
                .templateDataJson(data)
                .dataFields(NotificationSpecificFields.builder()
                        .emailDisplayName("Security Shortage")
                        .build())
                .build();
        transport.send(emailRoute, req);
    }

    public void sendSms(List<String> receivers, ShortageSMSTemplateData data) {
        var req = NotificationMessageRequest.<ShortageSMSTemplateData>builder()
                .receivers(receivers)
                .sender(smsSender)
                .subject(null)
                .templateName(smsTemplate)
                .templateDataJson(data)
                .build();
        transport.send(smsRoute, req);
    }
}
