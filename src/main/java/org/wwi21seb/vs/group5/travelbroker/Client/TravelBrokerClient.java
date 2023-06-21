package org.wwi21seb.vs.group5.travelbroker.Client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.UDP.Operation;
import org.wwi21seb.vs.group5.UDP.UDPMessage;

public class TravelBrokerClient {

    private final ConcurrentHashMap<UUID, CompletableFuture<UDPMessage>> pendingRequests;
    private final DatagramSocket socket;
    private final byte[] receiveBuffer = new byte[4096];
    private byte[] sendBuffer;
    private final ObjectMapper mapper;

    public TravelBrokerClient(int port) throws SocketException {
        socket = new DatagramSocket(port);
        mapper = new ObjectMapper();
        pendingRequests = new ConcurrentHashMap<>();
    }

    public CompletableFuture<UDPMessage> sendPacket(InetAddress address, int port, UDPMessage msg) throws IOException {
        CompletableFuture<UDPMessage> future = new CompletableFuture<>();
        pendingRequests.put(msg.getTransactionId(), future);

        String jsonString = mapper.writeValueAsString(msg);
        sendBuffer = jsonString.getBytes();
        DatagramPacket packet = new DatagramPacket(sendBuffer, sendBuffer.length, address, port);

        socket.send(packet);

        // Set timeout of 10 seconds
        future.orTimeout(10, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    pendingRequests.remove(msg.getTransactionId());
                    System.out.printf("Request %s timed out!%n", msg.getTransactionId());
                    return null;
                });

        return future;
    }

    public void startReceiving() {
        Thread thread = new Thread(() -> {
            while (true) {
                DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    socket.receive(packet);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                String received = new String(packet.getData(), 0, packet.getLength());
                System.out.printf("Received: %s%n", received);

                // Convert received data to UDPMessage
                UDPMessage msg = null;
                try {
                    msg = mapper.readValue(received, UDPMessage.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Check if message is a response to a request
                if (pendingRequests.containsKey(msg.getTransactionId())) {
                    // Complete future
                    pendingRequests.get(msg.getTransactionId()).complete(msg);
                } else {
                    // Handle message
                    switch (msg.getOperation()) {
                        case PREPARE -> {
                            System.out.println("TravelBrokerClient: Received PREPARE!");
                        }
                        case COMMIT -> {
                            System.out.println("TravelBrokerClient: Received COMMIT!");
                        }
                        case ABORT -> {
                            System.out.println("TravelBrokerClient: Received ABORT!");
                        }
                        case GET_BOOKINGS -> {
                            System.out.println("TravelBrokerClient: Received GET_BOOKINGS!");
                        }
                        case GET_AVAILABILITY -> {
                            System.out.println("TravelBrokerClient: Received GET_AVAILABILITY!");
                        }
                        default -> System.out.println("TravelBrokerClient: Unknown operation!");
                    }
                }
            }
        }, "TravelBrokerClient");

        thread.start();
    }

    public CompletableFuture<List<Object>> getHotelAvailability(AvailabilityRequest hotelAvailabilityRequest) {
        // Fetch from Hotel Provider Socket on Port 5002 via UDP
        try {
            String availabilityJsonString = mapper.writeValueAsString(hotelAvailabilityRequest);
            UDPMessage message = new UDPMessage(Operation.GET_AVAILABILITY, UUID.randomUUID(), "TravelBroker", availabilityJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue((String) msg.getData(), new TypeReference<List<Object>>() {});
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<List<Object>> getHotelBookings() {
        try {
            UDPMessage message = new UDPMessage(Operation.GET_BOOKINGS, UUID.randomUUID(), "TravelBroker", null);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue((String) msg.getData(), new TypeReference<List<Object>>() {});
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<List<Object>> getCarAvailability(AvailabilityRequest carAvailabilityRequest) {
        try {
            String availabilityJsonString = mapper.writeValueAsString(carAvailabilityRequest);
            UDPMessage message = new UDPMessage(Operation.GET_AVAILABILITY, UUID.randomUUID(), "TravelBroker", availabilityJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue((String) msg.getData(), new TypeReference<List<Object>>() {});
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<List<Object>> getCarBookings() {
        try {
            UDPMessage message = new UDPMessage(Operation.GET_BOOKINGS, UUID.randomUUID(), "TravelBroker", null);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue((String) msg.getData(), new TypeReference<List<Object>>() {});
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}