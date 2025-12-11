package mk.ukim.finki.ds.orderingdistributedsystem.config;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectedEvent;

@Component
public class WebSocketEventListener {

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event){
        System.out.println("WebSocket connection established");
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionConnectedEvent event){
        System.out.println("WebSocket connection closed");
    }
}
