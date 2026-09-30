package com.JobController;

import java.util.List;
import java.util.Optional;

public interface Scheduler {
    Optional<NodeEntry> selectNode(Job job, List<NodeEntry> candidates);
}
