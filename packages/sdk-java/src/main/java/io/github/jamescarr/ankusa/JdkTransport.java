package io.github.jamescarr.ankusa;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** The default {@link Transport}: a {@link HttpClient} with HTTP/1.1 and redirects disabled. */
final class JdkTransport implements Transport {

  /**
   * HTTP/1.1 on purpose: the Ankusa listeners speak plain HTTP/1.1, and pinning it keeps the JDK
   * client from sending an h2c upgrade header the listeners would have to ignore.
   */
  static final JdkTransport DEFAULT =
      new JdkTransport(
          HttpClient.newBuilder()
              .version(HttpClient.Version.HTTP_1_1)
              .followRedirects(HttpClient.Redirect.NEVER)
              .build());

  private final HttpClient client;

  JdkTransport(HttpClient client) {
    this.client = client;
  }

  @Override
  public TransportResponse send(TransportRequest request) throws IOException, InterruptedException {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(request.uri())
            .timeout(request.timeout())
            .method(
                request.method(),
                request.body() == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(request.body()));
    request.headers().forEach(builder::header);

    // get(timeout), not just HttpRequest.timeout: the request timeout stops at the response
    // headers, while this bounds connect through the last body byte, as Go's DefaultTimeout does.
    CompletableFuture<HttpResponse<byte[]>> future =
        client.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofByteArray());

    HttpResponse<byte[]> response;
    try {
      response = future.get(request.timeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new HttpTimeoutException("no complete response within " + request.timeout());
    } catch (InterruptedException e) {
      future.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException ioException) {
        throw ioException;
      }
      throw new IOException(cause);
    }

    return new TransportResponse(response.statusCode(), response.headers().map(), response.body());
  }
}
