package com.JobController.scheduling;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PlacementRequirements {
    private static final PlacementRequirements NONE = new PlacementRequirements(List.of());

    private final List<PlacementConstraint> constraints;

    public PlacementRequirements(List<PlacementConstraint> constraints) {
        this.constraints = List.copyOf(
                Objects.requireNonNull(constraints, "Placement constraints must not be null"));
        if (this.constraints.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Placement constraints must not contain null");
        }
    }

    public static PlacementRequirements none() {
        return NONE;
    }

    public List<PlacementConstraint> getConstraints() {
        return constraints;
    }

    public boolean isSatisfiedBy(Map<String, String> workerLabels) {
        Objects.requireNonNull(workerLabels, "Worker labels must not be null");
        return constraints.stream().allMatch(constraint -> constraint.isSatisfiedBy(workerLabels));
    }
}
