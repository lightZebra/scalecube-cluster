package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocket;
import io.scalecube.transport.Address;
import reactor.core.publisher.Mono;

public interface ClientTransport {

  Mono<RSocket> create(Address address);
  
}
