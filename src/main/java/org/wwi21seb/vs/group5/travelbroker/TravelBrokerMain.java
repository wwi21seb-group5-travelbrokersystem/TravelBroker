package org.wwi21seb.vs.group5.travelbroker;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import javafx.stage.Stage;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.travelbroker.Client.TravelBrokerClient;

import java.io.IOException;
import java.net.SocketException;

public class TravelBrokerMain extends Application {

    private TravelBrokerClient client;
    private Scene bookingScene;
    private Scene getBookingsScene;
    private Scene travelBrokerScene;
    private Scene hotelProviderScene;
    private Scene carProviderScene;

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

        // Setup UDP client
        try {
            client = new TravelBrokerClient(5010);
            client.startReceiving();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
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

        VBox getBookingsBox = new VBox();
        ListView<String> bookingsListView = new ListView<>();
        getBookingsBox.getChildren().addAll(new Label("Your Bookings:"), bookingsListView);

        getBookingsPane.setCenter(getBookingsBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();
        getBookingsScene = new Scene(getBookingsPane, width, height);
    }

    private void setupBookingScene(Stage primaryStage) {
        BorderPane bookingPane = new BorderPane();
        bookingPane.setTop(createMenuBar(primaryStage));

        VBox bookingBox = new VBox();
        Label bookingLabel = new Label("Select Date Range:");
        DatePicker startDatePicker = createDatePicker();
        DatePicker endDatePicker = createDatePicker();
        Button searchButton = new Button("Search Availability");

        ListView<Object> hotelListView = new ListView<>();
        ListView<Object> carListView = new ListView<>();
        Button bookButton = new Button("Book");

        searchButton.setOnMouseClicked((e -> {
            String startDate = startDatePicker.getValue().toString();
            String endDate = endDatePicker.getValue().toString();
            int capacity = 1;

            AvailabilityRequest availabilityRequest = new AvailabilityRequest(startDate, endDate, capacity);

            client.getHotelAvailability(availabilityRequest).thenAccept(hotels -> {
                Platform.runLater(() -> {
                    hotelListView.getItems().clear();
                    hotelListView.getItems().addAll(hotels);
                });
            });

            client.getCarAvailability(availabilityRequest).thenAccept(carList -> {
                // This runs when the car list is available
                carListView.getItems().clear();
                carListView.getItems().addAll(carList);
            });
        }));

        bookingBox.getChildren().addAll(
                bookingLabel, startDatePicker, endDatePicker,
                searchButton, hotelListView, carListView, bookButton
        );
        bookingPane.setCenter(bookingBox);

        int width = (int) Screen.getPrimary().getBounds().getWidth();
        int height = (int) Screen.getPrimary().getBounds().getHeight();
        bookingScene = new Scene(bookingPane, width, height);
    }

    private DatePicker createDatePicker() {
        DatePicker datePicker = new DatePicker();

        datePicker.setDayCellFactory(picker -> new DateCell() {
            @Override
            public void updateItem(java.time.LocalDate date, boolean empty) {
                super.updateItem(date, empty);
                setDisable(empty || date.isBefore(java.time.LocalDate.now()));
            }
        });

        return datePicker;
    }

    public static void main(String[] args) {
        launch();
    }
}