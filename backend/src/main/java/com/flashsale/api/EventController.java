package com.flashsale.api;

import java.security.Principal;
import java.util.List;
import java.util.UUID;
import com.flashsale.api.Contracts.EventView;
import com.flashsale.messaging.OutboxService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {
    private final OutboxService outbox;
    public EventController(OutboxService outbox) { this.outbox = outbox; }

    @GetMapping("/failed")
    List<EventView> failed(Principal principal) { return outbox.failedEvents(principal.getName()); }

    @PostMapping("/{id}/retry")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void retry(Principal principal, @PathVariable UUID id) { outbox.retry(principal.getName(), id.toString()); }
}
