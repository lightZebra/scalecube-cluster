package io.scalecube.services.transport.rsocket;

import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;

public interface ServerTransport {

  Mono<InetSocketAddress> bind(int port);

  Mono<Void> stop();
  
}
