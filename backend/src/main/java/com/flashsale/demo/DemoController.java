package com.flashsale.demo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.*;
import com.flashsale.core.AdmissionGate;
import com.flashsale.core.SaleService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/demo/api")
@ConditionalOnProperty(name = "sale.demo-enabled", havingValue = "true")
public class DemoController {
    private final SaleService service;
    private final AdmissionGate admission;

    public DemoController(SaleService service, AdmissionGate admission) { this.service = service; this.admission = admission; }

    @ModelAttribute
    void localOnly(HttpServletRequest request) throws UnknownHostException {
        String origin = request.getHeader("Origin");
        if (!InetAddress.getByName(request.getRemoteAddr()).isLoopbackAddress()
                || (origin != null && !origin.equals(request.getScheme() + "://" + request.getHeader("Host")))) {
            throw new ApiException(HttpStatus.FORBIDDEN, "LOCAL_DEMO_ONLY", "演示接口仅供本机同源访问");
        }
    }

    @GetMapping("/activities")
    List<ActivityView> activities() { return service.activities("demo"); }

    @PostMapping("/activities")
    @ResponseStatus(HttpStatus.CREATED)
    ActivityView create(@Valid @RequestBody CreateActivity input) { return service.create("demo", input); }

    @PostMapping("/requests")
    @ResponseStatus(HttpStatus.ACCEPTED)
    ReservationView submit(@Valid @RequestBody Reserve input) {
        admission.check("demo", input.userId());
        return service.submit("demo", input);
    }

    @GetMapping("/requests")
    List<ReservationView> history(@RequestParam String userId) {
        if (userId.isBlank() || userId.length() > 128) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_USER", "请填写有效的演示用户编号");
        }
        return service.history("demo", userId);
    }

    @PostMapping("/requests/{id}/confirm")
    ResponseEntity<ReservationView> confirm(@PathVariable UUID id, @Valid @RequestBody Confirm input) {
        var result = service.confirm("demo", id.toString(), input.orderId());
        return ResponseEntity.status(result.state().equals("EXPIRED") ? HttpStatus.CONFLICT : HttpStatus.OK).body(result);
    }

    @PostMapping("/requests/{id}/release")
    ReservationView release(@PathVariable UUID id, @RequestBody java.util.Map<String, Object> ignored) {
        return service.release("demo", id.toString());
    }
}
