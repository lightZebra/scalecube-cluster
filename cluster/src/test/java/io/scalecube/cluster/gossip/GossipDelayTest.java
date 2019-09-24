package io.scalecube.cluster.gossip;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.TransportImpl;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class GossipDelayTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GossipDelayTest.class);

  private static final long gossipInterval = GossipConfig.DEFAULT_GOSSIP_INTERVAL;
  private static final int gossipFanout = GossipConfig.DEFAULT_GOSSIP_FANOUT;
  private static final int gossipRepeatMultiplier = GossipConfig.DEFAULT_GOSSIP_REPEAT_MULT;

  private Scheduler scheduler = Schedulers.newSingle("scheduler", true);

  @Test
  public void testMessageDelayMoreThanGossipSweepTime() throws InterruptedException {
    final NetworkEmulatorTransport transport1 = getNetworkEmulatorTransport(0, 3000);
    final NetworkEmulatorTransport transport2 = getNetworkEmulatorTransport(0, 3000);
    final NetworkEmulatorTransport transport3 = getNetworkEmulatorTransport(0, 100);

    final GossipProtocolImpl gossipProtocol1 =
        initGossipProtocol(
            transport1,
            Arrays.asList(transport1.address(), transport2.address(), transport3.address()));
    final GossipProtocolImpl gossipProtocol2 =
        initGossipProtocol(
            transport2,
            Arrays.asList(transport1.address(), transport2.address(), transport3.address()));
    final GossipProtocolImpl gossipProtocol3 =
        initGossipProtocol(
            transport3,
            Arrays.asList(transport1.address(), transport2.address(), transport3.address()));

    gossipProtocol1.listen().subscribe(message -> LOGGER.info("1: {}", message));
    gossipProtocol2.listen().subscribe(message -> LOGGER.info("2: {}", message));
    gossipProtocol3.listen().subscribe(message -> LOGGER.info("3: {}", message));

    for (int i = 0; i < 3; i++) {
      gossipProtocol1.spread(Message.fromData("message: " + i)).subscribe();
    }

    Thread.currentThread().join();
  }

  private NetworkEmulatorTransport getNetworkEmulatorTransport(int lostPercent, int meanDelay) {
    NetworkEmulatorTransport transport = new NetworkEmulatorTransport(TransportImpl.bindAwait());
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
