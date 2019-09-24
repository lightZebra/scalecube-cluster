package io.scalecube.cluster.gossip;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.net.Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class GossipDelayTest extends BaseTest {

  // Allow to configure gossip settings other than defaults
  private static final long gossipInterval /* ms */ = GossipConfig.DEFAULT_GOSSIP_INTERVAL;
  //  private static final long gossipInterval /* ms */ = 20;
  private static final int gossipFanout = GossipConfig.DEFAULT_GOSSIP_FANOUT;
  private static final int gossipRepeatMultiplier = GossipConfig.DEFAULT_GOSSIP_REPEAT_MULT;

  private Scheduler scheduler = Schedulers.newSingle("scheduler", true);

  @Test
  public void testTwoNodesWithDelayMoreThanSweepTime() throws InterruptedException {
    final long gossipTimeoutToSweep =
        ClusterMath.gossipTimeoutToSweep(gossipRepeatMultiplier, 2, gossipInterval);
    final int meanDelay = (int) (gossipTimeoutToSweep + gossipTimeoutToSweep / 2);

    final NetworkEmulatorTransport transport1 = getNetworkEmulatorTransport(0, meanDelay);
    final NetworkEmulatorTransport transport2 = getNetworkEmulatorTransport(0, meanDelay);

    final GossipProtocolImpl gossipProtocol1 =
        initGossipProtocol(transport1, Arrays.asList(transport1.address(), transport2.address()));
    final GossipProtocolImpl gossipProtocol2 =
        initGossipProtocol(transport2, Arrays.asList(transport1.address(), transport2.address()));

    final AtomicInteger gossipCounter = new AtomicInteger();

    gossipProtocol1
        .listen()
        .subscribe(
            message -> {
              System.out.println("first: " + message);
            });
    gossipProtocol2
        .listen()
        .subscribe(
            message -> {
              gossipCounter.incrementAndGet();
              System.out.println("second: " + message);
            });

    gossipProtocol1.spread(Message.fromData("test")).subscribe();

    Thread.sleep(4 * (gossipInterval + meanDelay));

    assertEquals(1, gossipCounter.get());
  }

  @Test
  public void test() throws InterruptedException {
    final List<GossipProtocolImpl> gossipProtocols = initGossipProtocols(3, 0, 0);

    for (int i = 0; i < gossipProtocols.size(); i++) {
      final GossipProtocolImpl gossipProtocol = gossipProtocols.get(i);

      int finalI = i;
      gossipProtocol
          .listen()
          .subscribe(message -> System.out.println("Num: " + finalI + " " + message));
    }

    final GossipProtocolImpl firstProtocol = gossipProtocols.iterator().next();

    firstProtocol.spread(Message.fromData("123")).subscribe(System.out::println);

    Thread.currentThread().join();
  }

  private List<GossipProtocolImpl> initGossipProtocols(int count, int lostPercent, int meanDelay) {
    final List<Transport> transports = initTransports(count, lostPercent, meanDelay);
    List<Address> members = new ArrayList<>();
    for (Transport transport : transports) {
      members.add(transport.address());
    }
    List<GossipProtocolImpl> gossipProtocols = new ArrayList<>();
    for (Transport transport : transports) {
      gossipProtocols.add(initGossipProtocol(transport, members));
    }
    return gossipProtocols;
  }

  private List<Transport> initTransports(int count, int lostPercent, int meanDelay) {
    List<Transport> transports = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      NetworkEmulatorTransport transport =
          i == count - 1
              ? getNetworkEmulatorTransport(lostPercent, 100)
              : getNetworkEmulatorTransport(lostPercent, meanDelay);
      transports.add(transport);
    }
    return transports;
  }

  private NetworkEmulatorTransport getNetworkEmulatorTransport(int lostPercent, int meanDelay) {
    NetworkEmulatorTransport transport = createTransport();
    transport.networkEmulator().setDefaultOutboundSettings(lostPercent, meanDelay);
    return transport;
  }

  private GossipProtocolImpl initGossipProtocol(Transport transport, List<Address> members) {
    GossipConfig gossipConfig =
        new GossipConfig()
            .gossipFanout(gossipFanout)
            .gossipInterval(gossipInterval)
            .gossipRepeatMult(gossipRepeatMultiplier);

    Member localMember =
        new Member("member-" + transport.address().port(), null, transport.address());

    Flux<MembershipEvent> membershipFlux =
        Flux.fromIterable(members)
            .filter(address -> !transport.address().equals(address))
            .map(address -> new Member("member-" + address.port(), null, address))
            .map(member -> MembershipEvent.createAdded(member, null, 0));

    GossipProtocolImpl gossipProtocol =
        new GossipProtocolImpl(localMember, transport, membershipFlux, gossipConfig, scheduler);
    gossipProtocol.start();
    return gossipProtocol;
  }
}
