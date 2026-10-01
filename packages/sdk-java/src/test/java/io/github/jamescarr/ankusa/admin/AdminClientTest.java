package io.github.jamescarr.ankusa.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.FakeTransport;
import io.github.jamescarr.ankusa.TransportRequest;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** The query strings and replay bodies the vectors do not pin. */
class AdminClientTest {

  private final FakeTransport transport =
      new FakeTransport(
          request ->
              switch (request.uri().getPath()) {
                case "/v1/dlq" -> FakeTransport.json(200, "{\"total\":0,\"entries\":[]}");
                case "/v1/quarantine" -> FakeTransport.json(200, "{\"entries\":[]}");
                case "/v1/dlq/replay" -> FakeTransport.json(200, "{\"replayed\":0}");
                default -> FakeTransport.json(404, "{}");
              });

  private final AdminClient admin =
      new AdminClient("http://h", ClientOptions.builder().transport(transport).build());

  @Test
  void dead_letter_parameters_go_out_in_a_fixed_order() {
    admin.listDeadLetters(
        ListDeadLettersParams.builder().sourceId("demo").since(1720000000000L).limit(10).build());

    assertEquals(
        "http://h/v1/dlq?source_id=demo&since=1720000000000&limit=10",
        transport.onlyRequest().uri().toString());
  }

  @Test
  void an_unset_dead_letter_parameter_is_left_out_rather_than_sent_empty() {
    admin.listDeadLetters(ListDeadLettersParams.builder().limit(10).build());

    assertEquals("/v1/dlq", transport.onlyRequest().uri().getPath());
    assertEquals("limit=10", transport.onlyRequest().uri().getQuery());
  }

  @Test
  void list_dead_letters_with_no_parameters_sends_no_query_string() {
    assertEquals(0, admin.listDeadLetters().total());
    assertEquals("http://h/v1/dlq", transport.onlyRequest().uri().toString());
    assertNull(transport.onlyRequest().uri().getQuery());
  }

  @Test
  void list_quarantined_sends_only_a_non_null_limit() {
    admin.listQuarantined(ListQuarantinedParams.builder().limit(5).build());

    assertEquals("http://h/v1/quarantine?limit=5", transport.onlyRequest().uri().toString());
  }

  @Test
  void replay_with_no_filter_sends_an_empty_object() {
    assertEquals(0, admin.replayDeadLetters().replayed());

    TransportRequest request = transport.onlyRequest();
    assertEquals("POST", request.method());
    assertEquals("/v1/dlq/replay", request.uri().getPath());
    assertEquals("{}", body(request));
  }

  @Test
  void replay_sends_exactly_the_filter_the_caller_set() {
    admin.replayDeadLetters(ReplayFilter.builder().sourceId("demo").since(5L).build());

    assertEquals("{\"source_id\":\"demo\",\"since\":5}", body(transport.onlyRequest()));
  }

  private static String body(TransportRequest request) {
    assertEquals("application/json", request.headers().get("content-type"));
    return new String(request.body(), StandardCharsets.UTF_8);
  }
}
