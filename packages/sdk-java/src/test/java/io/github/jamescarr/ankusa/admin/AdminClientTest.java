package io.github.jamescarr.ankusa.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.FakeTransport;
import io.github.jamescarr.ankusa.TransportRequest;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** The query strings, paths and replay bodies the vectors do not pin. */
class AdminClientTest {

  /** One replay job body, shared by every replay response below. */
  private static final String REPLAY =
      "{\"id\":\"0194f4a0-0000-7000-8000-0000000000aa\",\"kind\":\"dlq\",\"state\":\"running\","
          + "\"filter\":{\"source_id\":\"demo\"},\"rate\":500,\"max_lag_ms\":2000,"
          + "\"created_at\":1720000000000,\"updated_at\":1720000000000,\"finished_at\":null,"
          + "\"moved\":0,\"scanned\":0,\"skipped\":0,\"delivered\":0,\"dead\":0,\"error\":null}";

  private final FakeTransport transport =
      new FakeTransport(
          request ->
              switch (request.uri().getPath()) {
                case "/v1/dlq" -> FakeTransport.json(200, "{\"total\":0,\"entries\":[]}");
                case "/v1/quarantine" -> FakeTransport.json(200, "{\"entries\":[]}");
                case "/v1/replays" ->
                    "POST".equals(request.method())
                        ? FakeTransport.json(202, REPLAY)
                        : FakeTransport.json(200, "{\"replays\":[" + REPLAY + "]}");
                case "/v1/replays/0194f4a0-0000-7000-8000-0000000000aa" ->
                    FakeTransport.json(200, REPLAY);
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
  void create_replay_sends_exactly_the_spec_the_caller_set() {
    Replay replay =
        admin.createReplay(ReplaySpec.builder().kind("dlq").sourceId("demo").rate(500).build());

    assertEquals("0194f4a0-0000-7000-8000-0000000000aa", replay.id());

    TransportRequest request = transport.onlyRequest();
    assertEquals("POST", request.method());
    assertEquals("/v1/replays", request.uri().getPath());
    assertEquals("{\"kind\":\"dlq\",\"source_id\":\"demo\",\"rate\":500}", body(request));
  }

  @Test
  void get_replay_uses_the_id_as_one_path_segment() {
    assertEquals(
        "0194f4a0-0000-7000-8000-0000000000aa",
        admin.getReplay("0194f4a0-0000-7000-8000-0000000000aa").id());

    assertEquals(
        "/v1/replays/0194f4a0-0000-7000-8000-0000000000aa",
        transport.onlyRequest().uri().getPath());
  }

  @Test
  void list_replays_decodes_the_page() {
    assertEquals(1, admin.listReplays().replays().size());

    assertEquals("/v1/replays", transport.onlyRequest().uri().getPath());
    assertNull(transport.onlyRequest().body());
  }

  @Test
  void update_replay_sends_exactly_the_patch_the_caller_set() {
    Replay replay =
        admin.updateReplay(
            "0194f4a0-0000-7000-8000-0000000000aa", ReplayPatch.builder().state("paused").build());

    assertEquals("0194f4a0-0000-7000-8000-0000000000aa", replay.id());

    TransportRequest request = transport.onlyRequest();
    assertEquals("PATCH", request.method());
    assertEquals("/v1/replays/0194f4a0-0000-7000-8000-0000000000aa", request.uri().getPath());
    assertEquals("{\"state\":\"paused\"}", body(request));
  }

  private static String body(TransportRequest request) {
    assertEquals("application/json", request.headers().get("content-type"));
    return new String(request.body(), StandardCharsets.UTF_8);
  }
}
