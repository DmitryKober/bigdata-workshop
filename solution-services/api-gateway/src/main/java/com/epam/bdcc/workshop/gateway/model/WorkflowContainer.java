package com.epam.bdcc.workshop.gateway.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
public class WorkflowContainer {

    public final List<String> userIds;
    public final int numberOfRequests;
    private final List<Workflow> workflows;

    @JsonCreator
    public WorkflowContainer(@JsonProperty("userIds") List<String> userIds, @JsonProperty("numberOfRequests") int numberOfRequests, @JsonProperty("workflows") List<Workflow> workflows) {
        if (userIds == null || userIds.isEmpty()) {
            throw new IllegalArgumentException("At least one userId must be specified.");
        }
        this.userIds = userIds;

        if (numberOfRequests <= 0) {
            throw new IllegalArgumentException("At least one request must be required.");
        }
        this.numberOfRequests = numberOfRequests;

        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("At least one workflow must be specified.");
        }
        this.workflows = workflows;
    }

    public List<String> getUserIds() {
        return new ArrayList<>(userIds);
    }

    public List<Workflow> getWorkflows() {
        return new ArrayList<>(workflows);
    }

    public static class Workflow {
        private final List<Step> steps;

        @JsonCreator
        public Workflow(@JsonProperty("steps") List<Step> steps) {
            if (steps == null || steps.isEmpty()) {
                throw new IllegalArgumentException("At least one workflow step must be specified.");
            }
            this.steps = steps;
        }

        public List<Step> getSteps() {
            return new ArrayList<>(steps);
        }

        public static class Step {
            public final String serviceName;
            public final long invocationDelay;

            @JsonCreator
            public Step(@JsonProperty("serviceName")String serviceName, @JsonProperty("invocationDelay") long invocationDelay) {
                this.serviceName = serviceName;
                this.invocationDelay = invocationDelay;
            }
        }
    }
}
