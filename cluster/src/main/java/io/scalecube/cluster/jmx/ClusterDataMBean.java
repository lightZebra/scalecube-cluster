package io.scalecube.cluster.jmx;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ClusterDataMBean {
  String member();

  Integer incarnation();

  Map<String, String> metadata();

  List<String> aliveMembers(); // all alive members except local one

  List<String> deadMembers(); // let's keep recent N dead members for full-filling this method

  Set<String> suspectedMembers();
}
