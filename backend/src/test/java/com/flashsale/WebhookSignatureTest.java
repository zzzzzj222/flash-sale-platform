package com.flashsale;

import com.flashsale.messaging.OutboxDispatcher;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class WebhookSignatureTest {
    @Test
    void matchesRfc4231Vector() {
        assertThat(OutboxDispatcher.sign("Jefe", "what do ya want for nothing?"))
                .isEqualTo("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }
}
