package org.wwi21seb.vs.group5.travelbroker.Client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Model.Booking;
import org.wwi21seb.vs.group5.Model.Car;
import org.wwi21seb.vs.group5.Model.Rental;
import org.wwi21seb.vs.group5.Model.Room;
import org.wwi21seb.vs.group5.Request.*;
import org.wwi21seb.vs.group5.UDP.Operation;
import org.wwi21seb.vs.group5.UDP.UDPMessage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TravelBrokerClient {

    private final ConcurrentHashMap<UUID, CompletableFuture<UDPMessage>> pendingRequests;
    private final DatagramSocket socket;
    private final byte[] receiveBuffer = new byte[4096];
    private final ObjectMapper mapper;
    private byte[] sendBuffer;

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

        System.out.printf("Sending transaction %s to %s:%s: %s%n", msg.getTransactionId(), address, port, jsonString);

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
                UDPMessage msg;
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
                        case PREPARE -> System.out.println("TravelBrokerClient: Received PREPARE!");
                        case COMMIT -> System.out.println("TravelBrokerClient: Received COMMIT!");
                        case ABORT -> System.out.println("TravelBrokerClient: Received ABORT!");
                        case GET_BOOKINGS -> System.out.println("TravelBrokerClient: Received GET_BOOKINGS!");
                        case GET_AVAILABILITY -> System.out.println("TravelBrokerClient: Received GET_AVAILABILITY!");
                        default -> System.out.println("TravelBrokerClient: Unknown operation!");
                    }
                }
            }
        }, "TravelBrokerClient");

        thread.start();
    }

    public CompletableFuture<List<Room>> getHotelAvailability(AvailabilityRequest hotelAvailabilityRequest) {
        // Fetch from Hotel Provider Socket on Port 5002 via UDP
        try {
            String availabilityJsonString = mapper.writeValueAsString(hotelAvailabilityRequest);
            UDPMessage message = new UDPMessage(Operation.GET_AVAILABILITY, UUID.randomUUID(), "TravelBroker", availabilityJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue(msg.getData(), new TypeReference<>() {
                    });
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<List<Booking>> getHotelBookings() {
        try {
            UDPMessage message = new UDPMessage(Operation.GET_BOOKINGS, UUID.randomUUID(), "TravelBroker", null);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue(msg.getData(), new TypeReference<>() {
                    });
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<List<Car>> getCarAvailability(AvailabilityRequest carAvailabilityRequest) {
        try {
            String availabilityJsonString = mapper.writeValueAsString(carAvailabilityRequest);
            UDPMessage message = new UDPMessage(Operation.GET_AVAILABILITY, UUID.randomUUID(), "TravelBroker", availabilityJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue(msg.getData(), new TypeReference<>() {
                    });
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<List<Rental>> getCarBookings() {
        try {
            UDPMessage message = new UDPMessage(Operation.GET_BOOKINGS, UUID.randomUUID(), "TravelBroker", null);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue(msg.getData(), new TypeReference<>() {
                    });
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Boolean> book(HotelReservationRequest hotelReservationRequest, CarReservationRequest carReservationRequest) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        // Step 1: Prepare hotel and car
        CompletableFuture<PrepareResult> prepareHotel = prepareHotel(hotelReservationRequest);
        CompletableFuture<PrepareResult> prepareCar = prepareCar(carReservationRequest);

        // Step 2: Determine next step if both prepare responses arrive
        CompletableFuture.allOf(prepareHotel, prepareCar).thenRun(() -> {
            try {
                // If both prepare responses are successful commit, else abort
                if (prepareHotel.get() != null && prepareCar.get() != null) {
                    // Step 3: Book hotel and car
                    CompletableFuture<Boolean> commitHotel = commitHotel(prepareHotel.get());
                    CompletableFuture<Boolean> commitCar = commitCar(prepareCar.get());

                    // Step 4: Determine next step if both book steps are successful
                    CompletableFuture.allOf(commitHotel, commitCar).thenRun(() -> {
                        try {
                            if (commitHotel.get() && commitCar.get()) {
                                // Transaction was successful
                                result.complete(true);
                            } else {
                                // Transaction failed
                                result.complete(false);
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            result.completeExceptionally(e);
                        }
                    });
                } else {
                    // At least one prepare failed, abort transaction
                    CompletableFuture<Boolean> abortHotel = abortHotel(prepareHotel.get());
                    CompletableFuture<Boolean> abortCar = abortCar(prepareCar.get());

                    CompletableFuture.allOf(abortHotel, abortCar).thenRun(() -> {
                        result.complete(false);
                    });
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        return result;
    }


    public CompletableFuture<PrepareResult> prepareCar(CarReservationRequest carReservationRequest) {
        try {
            String carReservationJsonString = mapper.writeValueAsString(carReservationRequest);
            UDPMessage message = new UDPMessage(Operation.PREPARE, UUID.randomUUID(), "TravelBroker", carReservationJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue(msg.getData(), PrepareResult.class);
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<PrepareResult> prepareHotel(HotelReservationRequest hotelReservationRequest) {
        try {
            String hotelReservationJsonString = mapper.writeValueAsString(hotelReservationRequest);
            UDPMessage message = new UDPMessage(Operation.PREPARE, UUID.randomUUID(), "TravelBroker", hotelReservationJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    return mapper.readValue(msg.getData(), PrepareResult.class);
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Boolean> commitCar(PrepareResult prepareResult) {
        try {
            String prepareResultJsonString = mapper.writeValueAsString(prepareResult);
            UDPMessage message = new UDPMessage(Operation.COMMIT, prepareResult.getTransactionId(), "TravelBroker", prepareResultJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    TransactionResult transactionResult = mapper.readValue(msg.getData(), TransactionResult.class);
                    return transactionResult.isSuccess();
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Boolean> commitHotel(PrepareResult prepareResult) {
        try {
            String prepareResultJsonString = mapper.writeValueAsString(prepareResult);
            UDPMessage message = new UDPMessage(Operation.COMMIT, prepareResult.getTransactionId(), "TravelBroker", prepareResultJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    TransactionResult transactionResult = mapper.readValue(msg.getData(), TransactionResult.class);
                    return transactionResult.isSuccess();
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Boolean> abortCar(PrepareResult prepareResult) {
        try {
            String prepareResultJsonString = mapper.writeValueAsString(prepareResult);
            UDPMessage message = new UDPMessage(Operation.ABORT, prepareResult.getTransactionId(), "TravelBroker", prepareResultJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5001, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    TransactionResult transactionResult = mapper.readValue(msg.getData(), TransactionResult.class);
                    return transactionResult.isSuccess();
                } catch (JsonProcessingException e) {
                    System.out.printf("Error parsing JSON: %s%n in transaction %s", e.getMessage(), msg.getTransactionId());
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Boolean> abortHotel(PrepareResult prepareResult) {
        try {
            String prepareResultJsonString = mapper.writeValueAsString(prepareResult);
            UDPMessage message = new UDPMessage(Operation.ABORT, prepareResult.getTransactionId(), "TravelBroker", prepareResultJsonString);
            CompletableFuture<UDPMessage> future = sendPacket(InetAddress.getLoopbackAddress(), 5002, message);

            // Wait for response
            return future.thenApply(msg -> {
                try {
                    TransactionResult transactionResult = mapper.readValue(msg.getData(), TransactionResult.class);
                    return transactionResult.isSuccess();
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
