package com.flashsale;

import com.flashsale.api.Contracts.RequestMessage;
import com.flashsale.core.SaleService;
import com.flashsale.messaging.RequestConsumer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.dao.QueryTimeoutException;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchFallbackTest {
    /** 用空依赖替代数据库，只记录消费者调用了哪些申请。 */
    private static class FakeSaleService extends SaleService {
        final List<String> processed = new ArrayList<>();
        RuntimeException batchFailure;

        FakeSaleService() { super(null, null, null, null); }

        @Override
        public void processBatch(List<RequestMessage> messages) {
            if (batchFailure != null) { throw batchFailure; }
        }

        @Override
        public void process(RequestMessage message) {
            processed.add(message.requestId());
        }
    }

    private static Message message(String payload) {
        return new Message(payload.getBytes(StandardCharsets.UTF_8), new MessageProperties());
    }

    @Test
    void reprocessesEachRequestWhenBatchHitsBadData() {
        var service = new FakeSaleService();
        service.batchFailure = new IllegalStateException("Reservation stock invariant violated");

        new RequestConsumer(service, new ObjectMapper()).receive(message("""
                [{"tenantId":"partner","requestId":"a","reservation":null},
                 {"tenantId":"partner","requestId":"b","reservation":null}]
                """));

        assertThat(service.processed).containsExactly("a", "b");
    }

    @Test
    void keepsBatchRetryForDatabaseFailures() {
        var service = new FakeSaleService();
        service.batchFailure = new QueryTimeoutException("database timeout");

        assertThatThrownBy(() -> new RequestConsumer(service, new ObjectMapper())
                .receive(message("[{\"tenantId\":\"partner\",\"requestId\":\"a\",\"reservation\":null}]")))
                .isInstanceOf(QueryTimeoutException.class);
        assertThat(service.processed).isEmpty();
    }
}
