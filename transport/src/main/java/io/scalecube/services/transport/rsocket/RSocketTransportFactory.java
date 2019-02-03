package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Executor;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;

/**
 * RSocket service transport. Entry point for getting {@link RSocketClientTransport} and {@link
 * RSocketServerTransport}.
 */
public class RSocketTransportFactory {
  
  public Resources resources(int numOfWorkers) {
    return new Resources(numOfWorkers);
  }

  public ClientTransport clientTransport(Resources resources) {
    return new RSocketClientTransport(DelegatedLoopResources
        .newClientLoopResources(((Resources) resources).workerPool));
  }

  public ServerTransport serverTransport(Resources resources) {
    return new RSocketServerTransport(((Resources) resources).workerPool);
  }

  /** RSocket service transport Resources implementation. Holds inside custom EventLoopGroup. */
  private static class Resources {

    private final EventLoopGroup workerPool;

    public Resources(int numOfWorkers) {
      workerPool =
          Epoll.isAvailable()
              ? new ExtendedEpollEventLoopGroup(numOfWorkers, this::chooseEventLoop)
              : new ExtendedNioEventLoopGroup(numOfWorkers, this::chooseEventLoop);
    }
    
    public Optional<Executor> workerPool() {
      return Optional.of(workerPool);
    }

    public Mono<Void> shutdown() {
      //noinspection unchecked
      return Mono.defer(
          () -> FutureMono.from((Future) ((EventLoopGroup) workerPool).shutdownGracefully()));
    }

    private EventLoop chooseEventLoop(Channel channel, Iterator<EventExecutor> executors) {
      while (executors.hasNext()) {
        EventExecutor eventLoop = executors.next();
        if (eventLoop.inEventLoop()) {
          return (EventLoop) eventLoop;
        }
      }
      return null;
    }
  }
}
