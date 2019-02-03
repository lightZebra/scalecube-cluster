package io.scalecube.services.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import java.util.Optional;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * RSocket service acceptor. Implementation of {@link SocketAcceptor}. See for details and supported
 * methods -- {@link AbstractRSocket0}.
 */
public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);


  public RSocketServiceAcceptor() {
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket socket) {
    LOGGER.info("Accepted rSocket: {}, connectionSetup: {}", socket, setup);
    return Mono.just(new AbstractRSocket() {});
  }
}
