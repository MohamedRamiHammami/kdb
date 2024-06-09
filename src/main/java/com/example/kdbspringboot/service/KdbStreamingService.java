package com.example.kdbspringboot.service;

import com.example.kdbspringboot.service.c.Flip;
import com.example.kdbspringboot.service.c.KException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class KdbStreamingService {

    private c tickerplantConnection;
    private c gatewayConnection;
    private ExecutorService executorService;
    private Object lastPollTime; // Store the last poll time to fetch incremental data

    @PostConstruct
    public void init() {
        try {
            // Connect to the tickerplant
            tickerplantConnection = new c("localhost", 5001); // Assuming tickerplant is on port 5001
            gatewayConnection = new c("localhost", 5002); // Assuming gateway is on port 5002
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(this::listenToTickerplant);
        } catch (IOException | KException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void close() {
        if (tickerplantConnection != null) {
            try {
                tickerplantConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (gatewayConnection != null) {
            try {
                gatewayConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private void listenToTickerplant() {
        try {
            while (true) {
                Object data = tickerplantConnection.k(); // Blocking call waiting for data from tickerplant
                if (data instanceof Flip) {
                    Flip flip = (Flip) data;
                    // Process the flip object, which contains the data
                    System.out.println(flip);
                }
            }
        } catch (KException | IOException e) {
            e.printStackTrace();
            // Switch to polling the gateway if tickerplant is down
            pollGateway();
        }
    }

    private void pollGateway() {
        try {
            while (true) {
                // Example query to retrieve data from the gateway
                String query = ".query.selectTable[`trade; `time within (lastPollTime; currentTime)]";
                Object data = gatewayConnection.k(query);
                if (data instanceof Flip) {
                    Flip flip = (Flip) data;
                    // Process the flip object, which contains the data
                    System.out.println(flip);
                }

                // Update lastPollTime to the current time after fetching data
                lastPollTime = System.currentTimeMillis() / 1000;

                // Sleep for a defined interval before the next poll
                TimeUnit.SECONDS.sleep(5);
            }
        } catch (KException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
