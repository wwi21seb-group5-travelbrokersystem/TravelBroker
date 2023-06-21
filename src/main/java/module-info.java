module org.wwi21seb.vs.group5.travelbroker {
    requires javafx.controls;
    requires javafx.fxml;
        requires javafx.web;
            
        requires org.controlsfx.controls;
            requires com.dlsc.formsfx;
            requires net.synedra.validatorfx;
            requires org.kordamp.ikonli.javafx;
            requires org.kordamp.bootstrapfx.core;
            // requires eu.hansolo.tilesfx; This somehow breaks the build
            requires com.almasb.fxgl.all;
            requires com.fasterxml.jackson.databind;
    requires SharedUtilities;

    opens org.wwi21seb.vs.group5.travelbroker to javafx.fxml;
    exports org.wwi21seb.vs.group5.travelbroker;
}