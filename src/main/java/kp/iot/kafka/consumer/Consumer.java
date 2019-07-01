package kp.iot.kafka.consumer;


import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import kp.iot.kafka.consumer.model.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.lang.reflect.Type;
import java.util.List;


@Service
@Slf4j
public class Consumer {



    // For Single Objects
    @KafkaListener(topics = "Kafka_Topic")
    public void jsonPacketConsumer(String receivedJsonPacket) {

        log.info("Packet Received : " + receivedJsonPacket);
        JsonParser parser = new JsonParser();
        String parsedJsonPacket = parser.parse(receivedJsonPacket).getAsString();

        Gson gson = new Gson();
        MessageDto messageDto = gson.fromJson(parsedJsonPacket, MessageDto.class);

        log.info("Packet Converted to Java Object : " + messageDto);
    }

    // For Array Objects
    @KafkaListener(topics = "Kafka_Topic_Array")
    public void jsonArrayConsumer(String receivedJsonPacket) {

        log.info("Packet Received : " + receivedJsonPacket);
        JsonParser parser = new JsonParser();
        String parsedJsonPacket = parser.parse(receivedJsonPacket).getAsString();

        Type collectionType = new TypeToken<List<MessageDto>>() {
        }.getType();
        List<MessageDto> receivedMessages = (List<MessageDto>) new Gson().fromJson(parsedJsonPacket, collectionType);

        log.info("Packet Converted to Java Array Object : " + receivedMessages);
    }
}