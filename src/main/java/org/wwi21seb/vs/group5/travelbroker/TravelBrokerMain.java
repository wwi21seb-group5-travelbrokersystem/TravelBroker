package org.wwi21seb.vs.group5.travelbroker;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;
import org.wwi21seb.vs.group5.Model.Booking;
import org.wwi21seb.vs.group5.Model.Car;
import org.wwi21seb.vs.group5.Model.Rental;
import org.wwi21seb.vs.group5.Model.Room;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.Request.ReservationRequest;
import org.wwi21seb.vs.group5.travelbroker.Server.TravelBrokerServer;

import java.net.SocketException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * The TravelBrokerMain class contains our UI and is the entry point of the
 * application. In the context of our Two-Phase-Commit protocol, this class
 * is the client.
 */
public class TravelBrokerMain extends Application {

    private TravelBrokerServer server;
    private Scene bookingScene;
    private Scene getBookingsScene;
    private Scene travelBrokerScene;
    private Scene hotelProviderScene;
    private Scene carProviderScene;

    private ObservableList<Car> cars = FXCollections.observableArrayList();
    private ObservableList<Room> rooms = FXCollections.observableArrayList();
    private ObservableList<Booking> bookings = FXCollections.observableArrayList();
    private ObservableList<Rental> rentals = FXCollections.observableArrayList();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String travelBrokerName = "TravelBroker";
    private final String hotelProviderName = "HotelProvider";
    private final String carProviderName = "CarProvider";

    public static void main(String[] args) {
        launch();
    }

    private MenuBar createMenuBar(Stage primaryStage) {
        // Setup menu
        MenuBar menuBar = new MenuBar();

        Menu bookingMenu = new Menu("Booking");
        MenuItem makeBooking = new MenuItem("Make Booking");
        MenuItem getBookings = new MenuItem("Get Bookings");
        bookingMenu.getItems().addAll(makeBooking, getBookings);

        Menu serviceMenu = new Menu("Service");
        MenuItem travelBroker = new MenuItem("Travel Broker");
        MenuItem hotelProvider = new MenuItem("Hotel Provider");
        MenuItem carProvider = new MenuItem("Car Provider");
        serviceMenu.getItems().addAll(travelBroker, hotelProvider, carProvider);

        menuBar.getMenus().addAll(bookingMenu, serviceMenu);

        // Assign scene switch actions
        makeBooking.setOnAction(e -> {
            primaryStage.setScene(bookingScene);
        });
        getBookings.setOnAction(e -> {
            server.getBookings().thenAccept(resultMap -> {
                Platform.runLater(() -> {
                    // This runs when the room list is available
                    bookings.clear();
                    rentals.clear();

                    resultMap.forEach((key, value) -> {
                        if (key.equals("HotelProvider")) {
                            bookings.addAll(value.stream().map(booking -> (Booking) booking).toList());
                        } else if (key.equals("CarProvider")) {
                            rentals.addAll(value.stream().map(rental -> (Rental) rental).toList());
                        }
                    });
                });
            });

            primaryStage.setScene(getBookingsScene);
        });
        travelBroker.setOnAction(e -> {
            primaryStage.setScene(travelBrokerScene);
        });
        hotelProvider.setOnAction(e -> {
            primaryStage.setScene(hotelProviderScene);
        });
        carProvider.setOnAction(e -> {
            primaryStage.setScene(carProviderScene);
        });

        return menuBar;
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Travel Booking System");

        // Setup UDP client
        try {
            server = new TravelBrokerServer(5000);
            server.startReceiving();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        // Setup booking scene
        setupBookingScene(primaryStage);

        // Setup get bookings scene
        setupGetBookingsScene(primaryStage);

        // Setup travel broker scene
        setupTravelBrokerScene(primaryStage);

        // Setup hotel provider scene
        setupHotelProviderScene(primaryStage);

        // Setup car provider scene
        setupCarProviderScene(primaryStage);

        // Setup initial scene with menu
        BorderPane borderPane = new BorderPane();
        borderPane.setTop(createMenuBar(primaryStage));
        Scene initialScene = new Scene(borderPane, 800, 600);

        primaryStage.setScene(initialScene);
        primaryStage.setMaximized(true);
        primaryStage.show();
    }

    private void setupCarProviderScene(Stage primaryStage) {
        BorderPane carProviderPane = new BorderPane();
        carProviderPane.setTop(createMenuBar(primaryStage));

        VBox carProviderBox = new VBox();
        carProviderBox.getChildren().add(new Label("Car Provider Scene"));

        carProviderPane.setCenter(carProviderBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();

        carProviderScene = new Scene(carProviderPane, width, height);
    }

    private void setupHotelProviderScene(Stage primaryStage) {
        BorderPane hotelProviderPane = new BorderPane();
        hotelProviderPane.setTop(createMenuBar(primaryStage));

        VBox hotelProviderBox = new VBox();
        hotelProviderBox.getChildren().add(new Label("Hotel Provider Scene"));

        hotelProviderPane.setCenter(hotelProviderBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();

        hotelProviderScene = new Scene(hotelProviderPane, width, height);
    }

    private void setupTravelBrokerScene(Stage primaryStage) {
        BorderPane travelBrokerPane = new BorderPane();
        travelBrokerPane.setTop(createMenuBar(primaryStage));

        VBox travelBrokerBox = new VBox();
        travelBrokerBox.getChildren().add(new Label("Travel Broker Scene"));

        travelBrokerPane.setCenter(travelBrokerBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();

        travelBrokerScene = new Scene(travelBrokerPane, width, height);
    }

    private void setupGetBookingsScene(Stage primaryStage) {
        BorderPane getBookingsPane = new BorderPane();
        getBookingsPane.setTop(createMenuBar(primaryStage));

        HBox bookingBox = new HBox(15);
        TableView<Booking> bookingTableView = new TableView<>();

        TableColumn<Booking, String> bookingIdIdcolumn = new TableColumn<>("Booking ID");
        bookingIdIdcolumn.setCellValueFactory(new PropertyValueFactory<>("id"));

        TableColumn<Booking, String> roomIdColumn = new TableColumn<>("Room ID");
        roomIdColumn.setCellValueFactory(new PropertyValueFactory<>("roomId"));

        TableColumn<Booking, Date> startDateColumn = new TableColumn<>("Start Date");
        startDateColumn.setCellValueFactory(new PropertyValueFactory<>("startDate"));

        TableColumn<Booking, Date> endDateColumn = new TableColumn<>("End Date");
        endDateColumn.setCellValueFactory(new PropertyValueFactory<>("endDate"));

        TableColumn<Booking, String> totalPriceColumn = new TableColumn<>("Total price");
        totalPriceColumn.setCellValueFactory(new PropertyValueFactory<>("totalPrice"));

        bookingTableView.getColumns().add(bookingIdIdcolumn);
        bookingTableView.getColumns().add(roomIdColumn);
        bookingTableView.getColumns().add(startDateColumn);
        bookingTableView.getColumns().add(endDateColumn);
        bookingTableView.getColumns().add(totalPriceColumn);
        bookingTableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        bookingTableView.setItems(bookings);

        TableView<Rental> carTableView = new TableView<>();

        TableColumn<Rental, String> rentalIdColumn = new TableColumn<>("Rental ID");
        rentalIdColumn.setCellValueFactory(new PropertyValueFactory<>("id"));

        TableColumn<Rental, String> carIdColumn = new TableColumn<>("Car ID");
        carIdColumn.setCellValueFactory(new PropertyValueFactory<>("car_id"));

        TableColumn<Rental, String> carStartDateColumn = new TableColumn<>("Start date");
        carStartDateColumn.setCellValueFactory(new PropertyValueFactory<>("start_date"));

        TableColumn<Rental, String> carEndDateColumn = new TableColumn<>("End date");
        carEndDateColumn.setCellValueFactory(new PropertyValueFactory<>("end_date"));

        TableColumn<Rental, String> carPriceColumn = new TableColumn<>("Total price");
        carPriceColumn.setCellValueFactory(new PropertyValueFactory<>("total_price"));

        carTableView.getColumns().add(rentalIdColumn);
        carTableView.getColumns().add(carIdColumn);
        carTableView.getColumns().add(carStartDateColumn);
        carTableView.getColumns().add(carEndDateColumn);
        carTableView.getColumns().add(carPriceColumn);
        carTableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        carTableView.setItems(rentals);

        bookingBox.getChildren().addAll(bookingTableView, carTableView);
        getBookingsPane.setCenter(bookingBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();
        getBookingsScene = new Scene(getBookingsPane, width, height);
    }

    private void setupBookingScene(Stage primaryStage) {
        BorderPane bookingPane = new BorderPane();
        bookingPane.setTop(createMenuBar(primaryStage));

        Label selectedRoomLabel = new Label("None");
        Label selectedCarLabel = new Label("None");

        VBox wrapperBox = new VBox(15);

        HBox formBox = new HBox(15);
        Label bookingLabel = new Label("Select Date Range:");

        DatePicker startDatePicker = new DatePicker(LocalDate.now());
        DatePicker endDatePicker = new DatePicker(LocalDate.now().plus(1, ChronoUnit.DAYS));

        startDatePicker.setDayCellFactory(picker -> new DateCell() {
            @Override
            public void updateItem(java.time.LocalDate date, boolean empty) {
                super.updateItem(date, empty);
                setDisable(empty || date.isBefore(java.time.LocalDate.now()));
            }
        });
        startDatePicker.valueProperty().addListener((observable, oldValue, newValue) -> {
            if (endDatePicker.valueProperty().get().isBefore(newValue.plus(1, ChronoUnit.DAYS))) {
                endDatePicker.setValue(newValue.plus(1, ChronoUnit.DAYS));
            }
        });
        endDatePicker.setDayCellFactory(picker -> new DateCell() {
            @Override
            public void updateItem(java.time.LocalDate date, boolean empty) {
                super.updateItem(date, empty);
                // Disable all dates before start date and start date itself
                setDisable(empty || date.isBefore(startDatePicker.getValue())
                        || date.isEqual(startDatePicker.getValue()));
            }
        });

        Label capacityLabel = new Label("Capacity:");
        Spinner<Integer> capacitySpinner = new Spinner<>(1, 10, 1);
        Button searchButton = new Button("Search Availability");

        HBox bookingBox = new HBox(15);
        TableView<Room> hotelTableView = new TableView<>();

        TableColumn<Room, String> roomIdColumn = new TableColumn<>("Room ID");
        roomIdColumn.setCellValueFactory(new PropertyValueFactory<>("id"));

        TableColumn<Room, String> roomTypeColumn = new TableColumn<>("Room Type");
        roomTypeColumn.setCellValueFactory(new PropertyValueFactory<>("type"));

        TableColumn<Room, String> roomCapacityColumn = new TableColumn<>("Capacity");
        roomCapacityColumn.setCellValueFactory(new PropertyValueFactory<>("capacity"));

        TableColumn<Room, String> roomPriceColumn = new TableColumn<>("Price per night");
        roomPriceColumn.setCellValueFactory(new PropertyValueFactory<>("pricePerNight"));

        hotelTableView.getColumns().add(roomIdColumn);
        hotelTableView.getColumns().add(roomTypeColumn);
        hotelTableView.getColumns().add(roomCapacityColumn);
        hotelTableView.getColumns().add(roomPriceColumn);
        hotelTableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        hotelTableView.setItems(rooms);
        hotelTableView.getSelectionModel().selectedItemProperty().addListener((obs, oldSelection, newSelection) -> {
            if (newSelection != null) {
                selectedRoomLabel.setText(newSelection.getId().toString());
            }
        });

        TableView<Car> carTableView = new TableView<>();

        TableColumn<Car, String> carIdColumn = new TableColumn<>("Car ID");
        carIdColumn.setCellValueFactory(new PropertyValueFactory<>("id"));

        TableColumn<Car, String> carModelColumn = new TableColumn<>("Model");
        carModelColumn.setCellValueFactory(new PropertyValueFactory<>("model"));

        TableColumn<Car, String> carManufacturerColumn = new TableColumn<>("Manufacturer");
        carManufacturerColumn.setCellValueFactory(new PropertyValueFactory<>("manufacturer"));

        TableColumn<Car, String> carCapacityColumn = new TableColumn<>("Capacity");
        carCapacityColumn.setCellValueFactory(new PropertyValueFactory<>("capacity"));

        TableColumn<Car, String> carPriceColumn = new TableColumn<>("Price per day");
        carPriceColumn.setCellValueFactory(new PropertyValueFactory<>("pricePerDay"));

        carTableView.getColumns().add(carIdColumn);
        carTableView.getColumns().add(carModelColumn);
        carTableView.getColumns().add(carManufacturerColumn);
        carTableView.getColumns().add(carCapacityColumn);
        carTableView.getColumns().add(carPriceColumn);
        carTableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        carTableView.setItems(cars);
        carTableView.getSelectionModel().selectedItemProperty().addListener((obs, oldSelection, newSelection) -> {
            if (newSelection != null) {
                selectedCarLabel.setText(newSelection.getId().toString());
            }
        });

        Button bookButton = new Button("Book");

        searchButton.setOnMouseClicked((e -> {
            String startDate = startDatePicker.getValue().toString();
            String endDate = endDatePicker.getValue().toString();
            int capacity = capacitySpinner.getValue();

            AvailabilityRequest availabilityRequest = new AvailabilityRequest(startDate, endDate, capacity);
            server.getAvailability(availabilityRequest).thenAccept(resultMap -> {
                Platform.runLater(() -> {
                    // This runs when the room list is available
                    // First clear the existing values of the lists
                    rooms.clear();
                    cars.clear();

                    // Get the result lists
                    List<Room> roomList = resultMap.get(hotelProviderName).stream().map(room -> (Room) room).toList();
                    List<Car> carList = resultMap.get(carProviderName).stream().map(car -> (Car) car).toList();

                    System.out.println(roomList);
                    System.out.println(carList);

                    // Add the new result lists
                    rooms.addAll(roomList);
                    cars.addAll(carList);
                });
            });
        }));

        bookButton.setOnMouseClicked((e -> {
            if (selectedRoomLabel.getText().equals("None") || selectedCarLabel.getText().equals("None")) {
                Alert alert = new Alert(Alert.AlertType.ERROR);
                alert.setTitle("Error");
                alert.setHeaderText("No room or car selected");
                alert.setContentText("Please select a room and a car before booking");
                alert.showAndWait();
            } else {
                String startDate = startDatePicker.getValue().toString();
                String endDate = endDatePicker.getValue().toString();
                int capacity = capacitySpinner.getValue();
                UUID roomId = UUID.fromString(selectedRoomLabel.getText());
                UUID carId = UUID.fromString(selectedCarLabel.getText());

                ReservationRequest reservationRequest = new ReservationRequest(null, startDate, endDate, capacity);

                server.book(reservationRequest, roomId, carId).thenAccept(bookingResponse -> {
                    Platform.runLater(() -> {
                        // This runs when the booking response is available
                        Alert alert;

                        if (bookingResponse) {
                            alert = new Alert(Alert.AlertType.INFORMATION);
                            alert.setTitle("Success");
                            alert.setHeaderText("Booking successful");
                            // alert.setContentText("Your booking ID is " + bookingResponse.getBookingId());
                        } else {
                            alert = new Alert(Alert.AlertType.ERROR);
                            alert.setTitle("Error");
                            alert.setHeaderText("Booking failed");
                            alert.setContentText("Your booking could not be made");
                        }
                        alert.showAndWait();
                    });
                });
            }
        }));

        formBox.getChildren().addAll(bookingLabel, startDatePicker, endDatePicker, capacityLabel, capacitySpinner, searchButton);
        bookingBox.getChildren().addAll(
                hotelTableView, carTableView
        );
        HBox footerBox = new HBox(15);
        footerBox.getChildren().addAll(
                new Label("Selected Room: "), selectedRoomLabel,
                new Label("Selected Car: "), selectedCarLabel,
                bookButton
        );
        wrapperBox.getChildren().addAll(formBox, bookingBox, footerBox);

        bookingPane.setCenter(wrapperBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();
        bookingScene = new Scene(bookingPane, width, height);
    }
}