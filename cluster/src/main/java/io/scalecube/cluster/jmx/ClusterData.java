package io.scalecube.cluster.jmx;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

public class ClusterData implements ClusterDataMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterData.class);

  private static final int N = 3;
  private final Cluster cluster;

  UnicastProcessor<String> deadMembersProcessor =
      UnicastProcessor.create(Queues.<String>get(N).get());
  FluxSink<String> deadMembersSink = deadMembersProcessor.sink(OverflowStrategy.LATEST);

  public ClusterData(Cluster cluster) {
    this.cluster = cluster;
    cluster
        .listenMembership()
        .filter(MembershipEvent::isRemoved)
        .subscribe(n -> deadMembersSink.next(n.toString()), t -> {} /* ignore */);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = null;
    try {
      objectName = new ObjectName("io.scalecube.cluster:name=Cluster");
      server.registerMBean(this, objectName);
    } catch (Throwable th) {
      LOGGER.error("Failed to register jmx bean, reason: ", th);
    }
  }

  @Override
  public String member() {
    return cluster.member().toString();
  }

  @Override
  public Integer incarnation() {
    return cluster.incarnation();
  }

  @Override
  public Map<String, String> metadata() {
    return cluster.metadata();
  }

  @Override
  public List<String> aliveMembers() {
    return cluster.members().stream().map(Member::id).collect(Collectors.toList());
  }

  @Override
  public List<String> deadMembers() {
    return deadMembersProcessor.collectList().block();
  }

  @Override
  public Set<String> suspectedMembers() {
    return cluster.suspectedMembers();
  }
}
