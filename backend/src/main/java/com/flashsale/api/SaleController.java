package com.flashsale.api;

import java.security.Principal;
import java.util.List;
import java.util.UUID;
import com.flashsale.api.Contracts.*;
import com.flashsale.core.AdmissionGate;
import com.flashsale.core.SaleService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class SaleController {
    private final SaleService service;
    private final AdmissionGate admission;

    public SaleController(SaleService service, AdmissionGate admission) {
        this.service = service;
        this.admission = admission;
    }

    @PostMapping("/activities")
    @ResponseStatus(HttpStatus.CREATED)
    ActivityView create(Principal principal, @Valid @RequestBody CreateActivity input) {
        return service.create(principal.getName(), input);
    }

    @GetMapping("/activities")
    List<ActivityView> activities(Principal principal) { return service.activities(principal.getName()); }

    @GetMapping("/activities/{id}")
    ActivityView activity(Principal principal, @PathVariable UUID id) {
        return service.activity(principal.getName(), id.toString());
    }

    @PostMapping("/requests")
    @ResponseStatus(HttpStatus.ACCEPTED)
    ReservationView submit(Principal principal, @Valid @RequestBody Reserve input) {
        admission.check(principal.getName(), input.userId());
        return service.submit(principal.getName(), input);
    }

    @GetMapping("/requests/{id}")
    ReservationView result(Principal principal, @PathVariable UUID id) {
        return service.result(principal.getName(), id.toString());
    }

    @PostMapping("/requests/{id}/confirm")
    ResponseEntity<ReservationView> confirm(Principal principal, @PathVariable UUID id, @Valid @RequestBody Confirm input) {
        var result = service.confirm(principal.getName(), id.toString(), input.orderId());
        return ResponseEntity.status(result.state().equals("EXPIRED") ? HttpStatus.CONFLICT : HttpStatus.OK).body(result);
    }

    @PostMapping("/requests/{id}/release")
    ReservationView release(Principal principal, @PathVariable UUID id) {
        return service.release(principal.getName(), id.toString());
    }
}
