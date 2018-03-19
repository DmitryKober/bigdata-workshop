package com.epam.bdcc.workshop.verification.controller;

import com.epam.bdcc.workshop.verification.service.VerificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@RestController
public class VerificationController {

    public static final String CONTEXT = "/bdcc-workshop";
    public static final String SERVICE = "/verification";

    private VerificationService verificationService;

    public VerificationController(@Autowired VerificationService verificationService) {
        this.verificationService = verificationService;
    }

    @RequestMapping(path = CONTEXT + SERVICE, method = RequestMethod.POST)
    public ResponseEntity doProcessRequest(@RequestBody Map requestBody) {
        String userId = String.valueOf(requestBody.get("userId"));
        if (userId == null || userId.isEmpty()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("userId is not specified.");
        }
        String workflowId = String.valueOf(requestBody.get("workflowId"));
        if (workflowId == null || workflowId.isEmpty()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("workflowId is not specified.");
        }

        verificationService.handle(userId, workflowId);
        return new ResponseEntity(HttpStatus.OK);
    }

}
