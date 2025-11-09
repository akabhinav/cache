package com.distributedcache.redis.server;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.commands.CommandRegistry;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespEncoder;
import com.distributedcache.redis.protocol.RespParser;
import com.distributedcache.redis.protocol.RespValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles client connections and processes commands
 */
public class ClientHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket clientSocket;
    private final DataStore dataStore;

    public ClientHandler(Socket clientSocket, DataStore dataStore) {
        this.clientSocket = clientSocket;
        this.dataStore = dataStore;
    }

    @Override
    public void run() {
        logger.info("Client connected: {}", clientSocket.getRemoteSocketAddress());

        try (InputStream in = clientSocket.getInputStream();
             OutputStream out = clientSocket.getOutputStream()) {

            RespParser parser = new RespParser(in);

            while (!clientSocket.isClosed() && !Thread.currentThread().isInterrupted()) {
                try {
                    RespValue request = parser.parse();
                    byte[] response = processCommand(request);
                    out.write(response);
                    out.flush();
                } catch (IOException e) {
                    if (!clientSocket.isClosed()) {
                        logger.debug("Error reading from client", e);
                    }
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("Error handling client", e);
        } finally {
            try {
                clientSocket.close();
                logger.info("Client disconnected: {}", clientSocket.getRemoteSocketAddress());
            } catch (IOException e) {
                logger.error("Error closing client socket", e);
            }
        }
    }

    private byte[] processCommand(RespValue request) throws IOException {
        if (!(request instanceof RespValue.Array array) || array.isNull()) {
            return RespEncoder.error("ERR Protocol error: expected array");
        }

        List<RespValue> values = array.values();
        if (values.isEmpty()) {
            return RespEncoder.error("ERR Protocol error: empty command");
        }

        // Extract command name and arguments
        List<String> args = new ArrayList<>();
        for (RespValue value : values) {
            if (value instanceof RespValue.BulkString bs && !bs.isNull()) {
                args.add(bs.value());
            } else {
                return RespEncoder.error("ERR Protocol error: invalid command format");
            }
        }

        String commandName = args.get(0).toUpperCase();
        List<String> commandArgs = args.subList(1, args.size());

        logger.debug("Executing command: {} with args: {}", commandName, commandArgs);

        Command command = CommandRegistry.get(commandName);
        if (command == null) {
            return RespEncoder.error("ERR unknown command '" + commandName + "'");
        }

        try {
            return command.execute(dataStore, commandArgs);
        } catch (Exception e) {
            logger.error("Error executing command: {}", commandName, e);
            return RespEncoder.error("ERR " + e.getMessage());
        }
    }
}
