package org.wwi21seb.vs.group5.travelbroker.Server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Logger.LoggerFactory;
import org.wwi21seb.vs.group5.Model.Booking;
import org.wwi21seb.vs.group5.Model.Car;
import org.wwi21seb.vs.group5.Model.Rental;
import org.wwi21seb.vs.group5.Model.Room;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.Request.ReservationRequest;
import org.wwi21seb.vs.group5.Request.TransactionResult;
import org.wwi21seb.vs.group5.TwoPhaseCommit.*;
import org.wwi21seb.vs.group5.UDP.Operation;
import org.wwi21seb.vs.group5.UDP.UDPMessage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The TravelBrokerServer represents the server in our Travel Broker and
 * is responsible for coordinating the reservation and booking of our
 * resources via Two-Phase-Commit.
 */
public class TravelBrokerServer {
    // The logger is used to log messages to the console.
    private static final Logger LOGGER = LoggerFactory.setupLogger(TravelBrokerServer.class.getName());

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

        LOGGER.log(Level.INFO, String.format("Starting TravelBrokerServer on port %s", port));

        // We read all existing logs and store them in our contexts HashMap.
        // This happens after a crash to recover the state of our server.
        for (CoordinatorContext context : logWriter.readAllLogs()) {
            LOGGER.log(Level.INFO, "Recovered transaction {0}", context.getTransactionId());
            contexts.put(context.getTransactionId(), context);

            switch (context.getTransactionState()) {
                case PREPARE -> {
                    LOGGER.log(Level.INFO, "Transaction {0} is in the prepare state", context.getTransactionId());

                    // If the transaction is in the prepare state, we send a prepare message to all participants.
                    if (context.getParticipants().stream().allMatch(p -> p.getVote().equals(Vote.YES))) {
                        LOGGER.log(Level.INFO, "All participants voted yes for transaction {0}, committing...", context.getTransactionId());
                        sendCommit(context.getTransactionId());
                    } else {
                        LOGGER.log(Level.INFO, "Not all participants voted yes for transaction {0}, aborting...", context.getTransactionId());
                        sendAbort(context.getTransactionId());
                    }
                }
                case ABORT -> {
                    LOGGER.log(Level.INFO, "Transaction {0} is already aborted", context.getTransactionId());

                    if (context.getParticipants().stream().allMatch(Participant::isDone)) {
                        LOGGER.log(Level.INFO, "Transaction {0} is done", context.getTransactionId());
                        contexts.remove(context.getTransactionId());
                        logWriter.deleteLog(context.getTransactionId());
                    } else {
                        LOGGER.log(Level.INFO, "Transaction {0} is not done yet", context.getTransactionId());
                        sendAbort(context.getTransactionId());
                    }
                }
                case COMMIT -> {
                    LOGGER.log(Level.INFO, "Transaction {0} is already committed", context.getTransactionId());

                    if (context.getParticipants().stream().allMatch(Participant::isDone)) {
                        LOGGER.log(Level.INFO, "Transaction {0} is done", context.getTransactionId());
                        contexts.remove(context.getTransactionId());
                        logWriter.deleteLog(context.getTransactionId());
                    } else {
                        LOGGER.log(Level.INFO, "Transaction {0} is not done yet", context.getTransactionId());
                        sendCommit(context.getTransactionId());
                    }
                }
                default -> {
                    LOGGER.log(Level.WARNING, "Unknown transaction state {0}", context.getTransactionState());
                    contexts.remove(context.getTransactionId());
                    logWriter.deleteLog(context.getTransactionId());
                }
            }
        }

        coordinator = new Coordinator("TravelBroker", InetAddress.getLoopbackAddress(), port);
        participants = List.of(new Participant("CarProvider", InetAddress.getLoopbackAddress(), 5001), new Participant("HotelProvider", InetAddress.getLoopbackAddress(), 5002));
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
        future.orTimeout(5, TimeUnit.SECONDS).exceptionally(e -> {
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
                    case RESULT -> receiveResult(msg, packet.getAddress(), packet.getPort());
                    default -> LOGGER.log(Level.WARNING, "Received unknown operation!");
                }
            }
        }, "TravelBrokerClient");

        thread.start();
    }

    private void receiveResult(UDPMessage msg, InetAddress address, int port) {
        CoordinatorContext context = contexts.get(msg.getTransactionId());
        UDPMessage response;

        if (context == null) {
            LOGGER.log(Level.WARNING, "Received RESULT for unknown transaction {0}", msg.getTransactionId());
            // This is an unknown transaction, this should not happen
            // We handle this by sending an abort to the participant
            // This way we prevent unwanted side effects
            response = new UDPMessage(Operation.ABORT, msg.getTransactionId(), "TravelBroker", "");
        } else {
            switch (context.getTransactionState()) {
                case PREPARE -> {
                    LOGGER.log(Level.INFO, "Received RESULT for transaction {0} in PREPARE state", msg.getTransactionId());
                    // We received a result for a transaction that is in the PREPARE state
                    // Since we are in the PREPARE state, we have not yet received a result from all participants
                    // We need to wait for all participants to send their result
                    // We can ignore this message for now
                    response = null;
                }
                case ABORT -> {
                    LOGGER.log(Level.INFO, "Received RESULT for transaction {0} in ABORT state", msg.getTransactionId());
                    // We received a result for a transaction that is in the ABORT state
                    // We need to send the participant an abort message
                    sendAbort(msg.getTransactionId());
                    response = null;
                }
                case COMMIT -> {
                    LOGGER.log(Level.INFO, "Received RESULT for transaction {0} in COMMIT state", msg.getTransactionId());
                    // We received a result for a transaction that is in the COMMIT state
                    // We need to send the participant a commit message
                    sendCommit(msg.getTransactionId());
                    response = null;
                }
                default -> {
                    LOGGER.log(Level.WARNING, "Unknown transaction state {0}", context.getTransactionState());
                    contexts.remove(context.getTransactionId());
                    logWriter.deleteLog(context.getTransactionId());
                    // This is an unknown transaction state, this should not happen
                    // We handle this by sending an abort to the participant
                    // This way we prevent unwanted side effects
                    response = new UDPMessage(Operation.ABORT, msg.getTransactionId(), "TravelBroker", "");
                }
            }
        }

        if (response != null) {
            // IF the message is null, we don't need to send a response
            // Since we handle this in the underlying methods
            // Otherwise send it back to the address and port we received it from
            try {
                String messageJsonString = mapper.writeValueAsString(response);
                byte[] sendBuffer = messageJsonString.getBytes();
                DatagramPacket packet = new DatagramPacket(sendBuffer, sendBuffer.length, address, port);
                socket.send(packet);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error sending UDP packet: {0}", e.getMessage());
            }
        }
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
                LOGGER.log(Level.WARNING, "Error sending UDP packet: {0}", e.getMessage());
                throw new RuntimeException(e);
            }
        });

        // Wait for all futures to complete and return resulting future
        LOGGER.log(Level.INFO, "Waiting for all GET_AVAILABILITY requests to complete...");
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
                LOGGER.log(Level.WARNING, "Error sending UDP packet: {0}", e.getMessage());
                throw new RuntimeException(e);
            }
        });

        // Wait for all futures to complete and return resulting future
        return getMapCompletableFuture(futures, false);
    }

    public CompletableFuture<Boolean> book(ReservationRequest reservationRequest, UUID roomId, UUID carId) {
        // Generate a new transaction id for the 2PC
        UUID transactionId = UUID.randomUUID();

        // Clone the value of the participants, not the reference
        List<Participant> contextParticipants = participants.stream().map(participant -> new Participant(participant.getName(), participant.getUrl(), participant.getPort())).toList();

        // Create a new context for the 2PC
        CoordinatorContext context = new CoordinatorContext(transactionId, TransactionState.PREPARE, coordinator, contextParticipants);

        context.getParticipants().forEach(participant -> {
            // Create a new participant with the respective booking context for each participant
            // and add it to the context of the 2PC afterward
            UUID resourceId = participant.getName().equals("HotelProvider") ? roomId : carId;
            BookingContext bookingContext = new BookingContext(resourceId, reservationRequest.getStartDate(), reservationRequest.getEndDate(), reservationRequest.getNumberOfPersons());

            participant.setBookingContext(bookingContext);
            CompletableFuture<Boolean> prepareFuture = new CompletableFuture<>();
            prepareFuture.orTimeout(10, TimeUnit.SECONDS).exceptionally(e -> {
                LOGGER.log(Level.WARNING, String.format("Prepare timeout for %s and transaction %s", participant.getName(), transactionId));
                // Since this is a timeout, we need to abort the transaction
                sendAbort(transactionId);
                return false;
            });
            participant.setPrepareFuture(prepareFuture);
        });

        // Add a future to the context of the 2PC which will be completed when the 2PC is finished
        // With this we can inform the client about the result of the 2PC
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        context.setSuccess(future);

        // Write the context to the log, this is to ensure that the context is not lost in case of a crash
        logWriter.writeLog(transactionId, context);
        contexts.put(transactionId, context);

        context.getParticipants().forEach(participant -> {
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

        // Check if the transaction is already finished, this happens when
        // we crash after the transaction is finished but before we can remove the context
        // To prevent a null pointer exception, we don't call the completable future, since
        // it does not get persisted in our log file
        if (!context.getTransactionState().equals(TransactionState.ABORT)) {
            // Complete the future with false to let our client know that the transaction failed
            // We can do this here because we know that the transaction failed
            // If the decision was ABORT, we would have to wait for the ACKs from the participants
            context.getSuccess().complete(false);
        }

        // Set the transaction state to ABORT
        context.setTransactionState(TransactionState.ABORT);

        // Write the log entry for the ABORT
        logWriter.writeLog(transactionId, context);

        context.getParticipants().forEach(participant -> {
            if (participant.isDone()) {
                // This participant has already responded with an ACK
                // So he's not affected by the timeout that caused
                // this method iteration
                LOGGER.log(Level.INFO, "Skipping {0} because he's already done", participant.getName());
                return;
            }

            // Send an ABORT request to each participant
            LOGGER.log(Level.INFO, "Sending ABORT to {0}", participant.getName());

            // Set the commitFuture which will time out if the participant doesn't respond in time
            // We would then continue to resend our decision until we get an ACK
            participant.resetCommitFuture();
            CompletableFuture<Boolean> commitFuture = participant.getCommitFuture();
            commitFuture.orTimeout(10, TimeUnit.SECONDS).exceptionally(e -> {
                LOGGER.log(Level.WARNING, String.format("Abort timeout for %s with transaction %s", participant.getName(), transactionId));
                // Resend the COMMIT
                sendAbort(transactionId);
                return null;
            });

            try {
                UDPMessage message = new UDPMessage(Operation.ABORT, transactionId, "TravelBroker", null);

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

        // In this case we can't complete the future with true because we don't know when the
        // transaction will succeed or not. We have to wait for the ACKs from the participants

        // Write the log entry for the COMMIT
        logWriter.writeLog(transactionId, context);

        context.getParticipants().forEach(participant -> {
            if (participant.isDone()) {
                // This participant has already responded with an ACK
                // So he's not affected by the timeout that caused
                // this method iteration
                return;
            }

            // Send a COMMIT request to each participant
            LOGGER.log(Level.INFO, "Sending COMMIT to {0}", participant.getName());

            // Set the commitFuture which will time out if the participant doesn't respond in time
            // We would then continue to resend our decision until we get an ACK
            participant.resetCommitFuture();
            CompletableFuture<Boolean> commitFuture = participant.getCommitFuture();
            commitFuture.orTimeout(10, TimeUnit.SECONDS).exceptionally(e -> {
                LOGGER.log(Level.WARNING, String.format("Commit timeout for %s with transaction %s", participant.getName(), transactionId));
                // Resend the COMMIT
                sendCommit(transactionId);
                return true;
            });

            try {
                UDPMessage message = new UDPMessage(Operation.COMMIT, transactionId, "TravelBroker", null);

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

    private void receivePrepare(UDPMessage message) {
        // Deserialize data to TransactionResult
        TransactionResult result;
        try {
            result = mapper.readValue(message.getData(), TransactionResult.class);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
            throw new RuntimeException(e);
        }

        // Find the context for this transaction
        CoordinatorContext context = contexts.get(message.getTransactionId());

        // Find the participant within the context
        Participant participant = context.getParticipants().stream().filter(p -> p.getName().equals(message.getSender())).findFirst().orElseThrow(() -> new RuntimeException("Unknown participant: " + message.getSender()));

        // Update the vote
        participant.setVote(result.isSuccess() ? Vote.YES : Vote.NO);
        // Mark the participant as having voted and cancel the timeout for this participant
        participant.getPrepareFuture().complete(true);

        // If all participants have responded, evaluate the votes
        if (context.getParticipants().stream().noneMatch((p) -> p.getVote().equals(Vote.PENDING))) {
            if (context.getParticipants().stream().allMatch((p) -> p.getVote().equals(Vote.YES))) {
                // All participants have voted YES, so we can commit
                sendCommit(message.getTransactionId());
            } else {
                // At least one participant has voted NO, so we have to abort
                sendAbort(message.getTransactionId());
            }
        } else {
            logWriter.writeLog(message.getTransactionId(), context);
        }
    }

    private void receiveCommit(UDPMessage message) {
        // Find the context for this transaction
        CoordinatorContext context = contexts.get(message.getTransactionId());

        if (context == null) {
            // We don't have a context for this transaction, so we can't commit
            // This can happen if we receive an COMMIT from a crashed participant
            // after we have already committed
            LOGGER.log(Level.WARNING, "Received COMMIT for unknown transaction {0}", message.getTransactionId());
            return;
        }

        // Parse message data to TransactionResult
        TransactionResult result;
        try {
            result = mapper.readValue(message.getData(), TransactionResult.class);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
            throw new RuntimeException(e);
        }

        // Update the participant if commit was successful
        if (result.isSuccess()) {
            context.setParticipants(context.getParticipants().stream().peek(p -> {
                if (p.getName().equals(message.getSender())) {
                    p.setDone();
                    // Cancel the timeout for this participant
                    p.getCommitFuture().complete(true);
                }
            }).toList());
        }

        // If all participants have responded, we can remove the context
        // and complete the future with true to let our client know that the transaction succeeded
        if (context.getParticipants().stream().allMatch(Participant::isDone)) {
            if (context.getSuccess() != null) {
                // If the context has a future, complete it
                // When we don't have one the coordinator crashed
                // and we don't have a way to communicate the result to the client
                context.getSuccess().complete(true);
            }
            contexts.remove(message.getTransactionId());
            logWriter.deleteLog(message.getTransactionId());
        } else {
            // Otherwise we update the transaction context for the participant
            logWriter.writeLog(message.getTransactionId(), context);
        }
    }

    private void receiveAbort(UDPMessage message) {
        // Find the context for this transaction
        CoordinatorContext context = contexts.get(message.getTransactionId());

        if (context == null) {
            // We don't have a context for this transaction, so we can't abort
            // This can happen if we receive an ABORT from a crashed participant
            // after we have already aborted
            LOGGER.log(Level.WARNING, "Received ABORT for unknown transaction {0}", message.getTransactionId());
            return;
        }

        // Parse message data to TransactionResult
        TransactionResult result;
        try {
            result = mapper.readValue(message.getData(), TransactionResult.class);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Error parsing JSON: {0}", e.getMessage());
            throw new RuntimeException(e);
        }

        // Update the participant if abort was successful
        if (result.isSuccess()) {
            context.setParticipants(context.getParticipants().stream().peek(p -> {
                if (p.getName().equals(message.getSender())) {
                    p.setDone();
                    // Cancel the timeout for this participant
                    p.getCommitFuture().complete(true);
                }
            }).toList());
        }


        // If all participants have responded, we can remove the context
        if (context.getParticipants().stream().allMatch(Participant::isDone)) {
            contexts.remove(message.getTransactionId());
            logWriter.deleteLog(message.getTransactionId());
        } else {
            // Otherwise we update the transaction context for the participant
            logWriter.writeLog(message.getTransactionId(), context);
        }
    }

    private CompletableFuture<Map<String, List<Object>>> getMapCompletableFuture(List<CompletableFuture<UDPMessage>> futures, boolean isAvailability) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply(response -> {
            Map<String, List<Object>> result = new HashMap<>();
            LOGGER.log(Level.INFO, "All responses received");

            futures.forEach(msg -> {
                try {
                    // Get UDPMessage from response
                    UDPMessage message = msg.get();
                    switch (message.getSender()) {
                        case "HotelProvider" -> {
                            LOGGER.log(Level.INFO, "Parsing HotelProvider response");

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
                            LOGGER.log(Level.INFO, "Parsing CarProvider response");

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

            LOGGER.log(Level.INFO, "Returning result {0}", result);
            return result;
        });
    }

}
