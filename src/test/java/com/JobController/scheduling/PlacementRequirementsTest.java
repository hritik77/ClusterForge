package com.JobController.scheduling;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PlacementRequirementsTest {
    @Test
    public void supportsRequiredEqualityAndInequalityConstraints() {
        PlacementRequirements requirements = new PlacementRequirements(List.of(
                new PlacementConstraint("architecture", PlacementOperator.EQUALS, "x86_64"),
                new PlacementConstraint("region", PlacementOperator.NOT_EQUALS, "mumbai")));

        assertTrue(requirements.isSatisfiedBy(Map.of(
                "architecture", "x86_64", "region", "ahmedabad")));
        assertFalse(requirements.isSatisfiedBy(Map.of(
                "architecture", "arm64", "region", "ahmedabad")));
        assertFalse(requirements.isSatisfiedBy(Map.of(
                "architecture", "x86_64", "region", "mumbai")));
        assertFalse(requirements.isSatisfiedBy(Map.of("architecture", "x86_64")));
    }

    @Test
    public void emptyRequirementsAcceptAnyWorkerAndInputIsImmutable() {
        assertTrue(PlacementRequirements.none().isSatisfiedBy(Map.of()));
        assertThrows(UnsupportedOperationException.class, () ->
                new PlacementRequirements(List.of(
                        new PlacementConstraint("disk", PlacementOperator.EQUALS, "ssd")))
                        .getConstraints().clear());
    }

    @Test
    public void rejectsInvalidConstraints() {
        assertThrows(IllegalArgumentException.class, () ->
                new PlacementConstraint(" ", PlacementOperator.EQUALS, "x86_64"));
        assertThrows(IllegalArgumentException.class, () ->
                new PlacementConstraint("architecture", PlacementOperator.EQUALS, " "));
        assertThrows(NullPointerException.class, () ->
                new PlacementConstraint("architecture", null, "x86_64"));
    }
}
