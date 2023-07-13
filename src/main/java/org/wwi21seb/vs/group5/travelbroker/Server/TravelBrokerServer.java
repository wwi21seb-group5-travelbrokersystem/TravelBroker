package org.wwi21seb.vs.group5.travelbroker.Server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Model.Booking;
import org.wwi21seb.vs.group5.Model.Car;
import org.wwi21seb.vs.group5.Model.Rental;
import org.wwi21seb.vs.group5.Model.Room;
import org.wwi21seb.vs.group5.Request.*;
import org.wwi21seb.vs.group5.TwoPhaseCommit.*;
import org.wwi21seb.vs.group5.UDP.Operation;
import org.wwi21seb.vs.group5.UDP.UDPMessage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

/**
 * The TravelBrokerServer represents the server in our Travel Broker and
 * is responsible for coordinating the reservation and booking of our
 * resources via Two-Phase-Commit.
 */
public class TravelBrokerServer {
    // The logger is used to log messages to the console.
    private static final Logger LOGGER = Logger.getLogger(TravelBrokerServer.class.getName());

    // The logWriter is used to write our Contexts to a file. This is used to
    // recover from a crash.
    private final LogWriter<CoordinatorContext> logWriter = new LogWriter<>();

    // The pendingRequests HashMap stores the current transactions and the action
    // that should be executed once the request is fulfilled. This is used to map
    // incoming UDP messages to its existing context.
    private final ConcurrentHashMap<UUID, CompletableFuture<UDPMessage>> pendingRequests;

    private final ConcurrentHashMap<UUID, CoordinatorContext> contexts = new ConcurrentHashMap<>();

    // The socket is used to receive and send messages via UDP.
    private final DatagramSocket socket;

    // The receive buffer is used to handle incoming messages.
    private final byte[] receiveBuffer = new byte[4096];

    // The mapper is used to parse our UDPMessages into strings and vice versa.
    private final ObjectMapper mapper;

    // The coordinator is used to initiate our Context with the Coordinator (in our case
    // the TravelBrokerServer (this class)).
    private final Coordinator coordinator;

    // The participants are also used to initialize our Context with the involved participants.
    // In our case the HotelProvider and CarProvider.
    private final List<Participant> participants;

    public TravelBrokerServer(int port) throws SocketException {
        socket = new DatagramSocket(port);
        mapper = new ObjectMapper();
        pendingRequests = new ConcurrentHashMap<>();

        // We read all existing logs and store them in our contexts HashMap.
        // This happens after a crash to recover the state of our server.
        for (CoordinatorContext context : logWriter.readAllLogs()) {
            LOGGER.log(Level.INFO, "Recovered transaction {0}", context.getTransactionId());
            contexts.put(context.getTransactionId(), context);
        }

        coordinator = new Coordinator("TravelBroker", InetAddress.getLoopbackAddress(), 5000);
        participants = List.of(
                new Participant(
                        "CarProvider", InetAddress.getLoopbackAddress(), 5001
                ),
                new Participant(
                        "HotelProvider", InetAddress.getLoopbackAddress(), 5002
                )
        );
    }

    public CompletableFuture<UDPMessage> sendPacket(InetAddress address, int port, UDPMessage msg) throws IOException {
        CompletableFuture<UDPMessage> future = new CompletableFuture<>();
        pendingRequests.put(msg.getTransactionId(), future);

        String jsonString = mapper.writeValueAsString(msg);
        byte[] sendBuffer = jsonString.getBytes();
        DatagramPacket packet = new DatagramPacket(sendBuffer, sendBuffer.length, address, port);

        LOGGER.log(Level.INFO, "Sending transaction {0} to {1}:{2}: {3}", new Object[]{msg.getTransactionId(), address, port, jsonString});

        socket.send(packet);

        // Set timeout of 10 seconds
        future.orTimeout(5, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    pendingRequests.remove(msg.getTransactionId());
                    LOGGER.log(Level.WARNING, "Timeout for transaction {0}", msg.getTransactionId());
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

                // Convert received data to UDPMessage
                UDPMessage msg;
                try {
                    msg = mapper.readValue(received, UDPMessage.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                LOGGER.log(Level.INFO, "Received transaction {0} from {1}, {2}", new Object[]{msg.getTransactionId(), msg.getSender(), msg.getOperation()});

                switch (msg.getOperation()) {
                    case GET_BOOKINGS, GET_AVAILABILITY -> {
                        if (pendingRequests.containsKey(msg.getTransactionId())) {
                            pendingRequests.get(msg.getTransactionId()).complete(msg);
                        } else {
                            LOGGER.log(Level.WARNING, "Received GET_BOOKINGS or GET_AVAILABILITY without pending request!");
                        }
                    }
                    case PREPARE -> receivePrepare(msg);
                    case COMMIT -> receiveCommit(msg);
                    case ABORT -> receiveAbort(msg);
                    default -> LOGGER.log(Level.WARNING, "Received unknown operation!");
                }
            }
        }, "TravelBrokerClient");

        thread.start();
    }

    public CompletableFuture<Map<String, List<Object>>> getAvailability(AvailabilityRequest availabilityRequest) {
        List<CompletableFuture<UDPMessage>> futures = new ArrayList<>();
        String availabilityJsonString = "";

        try {
            availabilityJsonString = mapper.writeValueAsString(availabilityRequest);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
            return null;
        }

        String finalAvailabilityJsonString = availabilityJsonString;
        participants.forEach(participant -> {
            // Send a GET_AVAILABILITY request to each participant
            UDPMessage message = new UDPMessage(Operation.GET_AVAILABILITY, UUID.randomUUID(), "TravelBroker", finalAvailabilityJsonString);

            try {
                // Add future to list
                futures.add(sendPacket(participant.getUrl(), participant.getPort(), message));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for all futures to complete and return resulting future
        System.out.println("Waiting for all futures to complete");
        return getMapCompletableFuture(futures, true);
    }

    public CompletableFuture<Map<String, List<Object>>> getBookings() {
        List<CompletableFuture<UDPMessage>> futures = new ArrayList<>();

        participants.forEach(participant -> {
            // Send a GET_BOOKINGS request to each participant
            UDPMessage message = new UDPMessage(Operation.GET_BOOKINGS, UUID.randomUUID(), "TravelBroker", null);

            try {
                // Add future to list
                futures.add(sendPacket(participant.getUrl(), participant.getPort(), message));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for all futures to complete and return resulting future
        return getMapCompletableFuture(futures, false);
    }

    public CompletableFuture<Boolean> book(ReservationRequest reservationRequest, UUID roomId, UUID carId) {
        // Generate a new transaction id for the 2PC
        UUID transactionId = UUID.randomUUID();
        // Create a new context for the 2PC
        CoordinatorContext context = new CoordinatorContext(transactionId, TransactionState.PREPARE, coordinator, participants);
        List<Participant> newParticipants = new ArrayList<>();

        participants.forEach(participant -> {
            // Create a new participant with the respective booking context for each participant
            // and add it to the context of the 2PC afterwards
            UUID resourceId = participant.getName().equals("HotelProvider") ? roomId : carId;
            Participant newParticipant = new Participant(participant.getName(), participant.getUrl(), participant.getPort());
            BookingContext bookingContext = new BookingContext(resourceId, reservationRequest.getStartDate(), reservationRequest.getEndDate(), reservationRequest.getNumberOfPersons());

            newParticipant.setBookingContext(bookingContext);
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.orTimeout(10, TimeUnit.SECONDS)
                    .thenApply(e -> {
                        LOGGER.log(Level.WARNING, "Timeout for transaction {0}", transactionId);
                        // Since this is a timeout, we need to abort the transaction
                        sendAbort(transactionId);
                        return false;
                    });
            newParticipant.setPrepareFuture(future);

            newParticipants.add(newParticipant);
        });

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        context.setSuccess(future);
        context.setParticipants(newParticipants);
        logWriter.writeLog(transactionId, context);

        participants.forEach(participant -> {
            // Send a PREPARE request to each participant
            LOGGER.log(Level.INFO, "Sending PREPARE to {0}", participant.getName());

            try {
                String contextJsonString = mapper.writeValueAsString(context);
                UDPMessage message = new UDPMessage(Operation.PREPARE, transactionId, "TravelBroker", contextJsonString);

                String messageJsonString = mapper.writeValueAsString(message);
                byte[] sendBuffer = messageJsonString.getBytes();
                DatagramPacket packet = new DatagramPacket(sendBuffer, sendBuffer.length, participant.getUrl(), participant.getPort());
                socket.send(packet);
            } catch (JsonProcessingException e) {
                LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error sending packet: {0}", e.getMessage());
                throw new RuntimeException(e);
            }
        });

        return context.getSuccess();
    }

    public void sendAbort(UUID transactionId) {
        // Get the existing context for the transaction
        CoordinatorContext context = contexts.get(transactionId);

        // Set the transaction state to ABORT
        context.setTransactionState(TransactionState.ABORT);

        // Complete the future with false to let our client know that the transaction failed
        context.getSuccess().complete(false);

        // Write the log entry for the ABORT
        logWriter.writeLog(transactionId, context);

        participants.forEach(participant -> {
            // Send an ABORT request to each participant
            LOGGER.log(Level.INFO, "Sending ABORT to {0}", participant.getName());

            try {
                String contextJsonString = mapper.writeValueAsString(context);
                UDPMessage message = new UDPMessage(Operation.ABORT, transactionId, "TravelBroker", contextJsonString);

                String messageJsonString = mapper.writeValueAsString(message);
                byte[] sendBuffer = messageJsonString.getBytes();
                DatagramPacket packet = new DatagramPacket(sendBuffer, sendBuffer.length, participant.getUrl(), participant.getPort());
                socket.send(packet);
            } catch (JsonProcessingException e) {
                LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error sending packet: {0}", e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    private void sendCommit(UUID transactionId) {
        // Get the existing context for the transaction
        CoordinatorContext context = contexts.get(transactionId);

        // Set the transaction state to COMMIT
        context.setTransactionState(TransactionState.COMMIT);

        // Complete the future with true to let our client know that the transaction succeeded
        context.getSuccess().complete(true);

        // Write the log entry for the COMMIT
        logWriter.writeLog(transactionId, context);

        participants.forEach(participant -> {
            // Send a COMMIT request to each participant
            LOGGER.log(Level.INFO, "Sending COMMIT to {0}", participant.getName());

            try {
                String contextJsonString = mapper.writeValueAsString(context);
                UDPMessage message = new UDPMessage(Operation.COMMIT, transactionId, "TravelBroker", contextJsonString);

                String messageJsonString = mapper.writeValueAsString(message);
                byte[] sendBuffer = messageJsonString.getBytes();
                DatagramPacket packet = new DatagramPacket(sendBuffer, sendBuffer.length, participant.getUrl(), participant.getPort());
                socket.send(packet);
            } catch (JsonProcessingException e) {
                LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error sending packet: {0}", e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    private void receivePrepare(Object message) {

    }

    private void receiveCommit(Object message) {

    }

    private void receiveAbort(Object message) {

    }

    private CompletableFuture<Map<String, List<Object>>> getMapCompletableFuture(List<CompletableFuture<UDPMessage>> futures, boolean isAvailability) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(response -> {
                    Map<String, List<Object>> result = new HashMap<>();
                    System.out.println("All futures completed");

                    futures.forEach(msg -> {
                        try {
                            // Get UDPMessage from response
                            UDPMessage message = msg.get();
                            switch (message.getSender()) {
                                case "HotelProvider" -> {
                                    System.out.println("Parsing HotelProvider response");

                                    if (isAvailability) {
                                        List<Room> rooms = mapper.readValue(message.getData(), new TypeReference<>() {
                                        });
                                        result.put(message.getSender(), new ArrayList<>(rooms));
                                    } else {
                                        List<Booking> bookings = mapper.readValue(message.getData(), new TypeReference<>() {
                                        });
                                        result.put(message.getSender(), new ArrayList<>(bookings));
                                    }
                                }
                                case "CarProvider" -> {
                                    System.out.println("Parsing CarProvider response");

                                    if (isAvailability) {
                                        List<Car> cars = mapper.readValue(message.getData(), new TypeReference<>() {
                                        });
                                        result.put(message.getSender(), new ArrayList<>(cars));
                                    } else {
                                        List<Rental> rentals = mapper.readValue(message.getData(), new TypeReference<>() {
                                        });
                                        result.put(message.getSender(), new ArrayList<>(rentals));
                                    }
                                }
                                default -> throw new IllegalStateException("Unexpected sender: " + message.getSender());
                            }
                        } catch (ExecutionException e) {
                            LOGGER.log(Level.WARNING, "Error while waiting for response", e);
                            throw new RuntimeException(e);
                        } catch (InterruptedException e) {
                            LOGGER.log(Level.WARNING, "Interrupted while waiting for response", e);
                            throw new RuntimeException(e);
                        } catch (JsonProcessingException e) {
                            LOGGER.log(Level.WARNING, "Error parsing JSON", e);
                            throw new RuntimeException(e);
                        }
                    });

                    System.out.println("Returning result");
                    return result;
                });
    }

}
