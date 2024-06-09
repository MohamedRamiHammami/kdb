package com.example.kdbspringboot.service;

import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

@Service
public class KdbService {

    private c connection;

    @PostConstruct
    public void init() {
        try {
            // Connect to the Kdb+ database
            connection = new c("localhost", 5000, "username:password");
        } catch (c.KException | IOException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Object executeQuery(String query) {
        try {
            return connection.k(query);
        } catch (c.KException | IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
