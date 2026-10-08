package com.JobController.scheduling;

import java.util.Map;
import java.util.Objects;

public final class PlacementConstraint {
    private final String key;
    private final PlacementOperator operator;
    private final String value;

    public PlacementConstraint(String key, PlacementOperator operator, String value) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Placement constraint key must not be blank");
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Placement constraint value must not be blank");
        }
        this.key = key;
        this.operator = Objects.requireNonNull(operator, "Placement operator must not be null");
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public PlacementOperator getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PlacementConstraint constraint)) {
            return false;
        }
        return key.equals(constraint.key)
                && operator == constraint.operator
                && value.equals(constraint.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, operator, value);
    }

    boolean isSatisfiedBy(Map<String, String> labels) {
        String label = labels.get(key);
        if (label == null) {
            return false;
        }
        return switch (operator) {
            case EQUALS -> label.equals(value);
            case NOT_EQUALS -> !label.equals(value);
        };
    }
}
