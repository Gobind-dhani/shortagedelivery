package com.indiabulls.shortagedelivery.notification.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RabbitMqTransportService implements NotificationTransportService {

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
