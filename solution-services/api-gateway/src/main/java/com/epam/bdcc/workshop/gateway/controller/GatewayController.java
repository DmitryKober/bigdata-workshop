package com.epam.bdcc.workshop.gateway.controller;

import com.epam.bdcc.workshop.gateway.model.WorkflowContainer;
import com.epam.bdcc.workshop.gateway.service.WorkflowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@RestController
public class GatewayController {

    public static final String CONTEXT = "/bdcc-workshop";
    public static final String GATEWAY_SERVICE = CONTEXT + "/gateway";

    private WorkflowService workflowService;

    public GatewayController(@Autowired WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @RequestMapping(path = GATEWAY_SERVICE, method = RequestMethod.POST)
    void doProcessRequest(@RequestBody WorkflowContainer workflowContainer) {
        workflowService.handle(workflowContainer);
    }

}
