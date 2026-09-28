package com.hashanchor.api;

import com.hashanchor.domain.VerificationService;
import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

/**
 * {@code POST /api/verify}: upload a document, learn whether that exact
 * content is anchored on-chain. "Not anchored" is a normal answer, not an
 * error, so both outcomes are {@code 200 OK} and the body's
 * {@code anchored} field says which.
 */
@RestController
@RequestMapping("/api/verify")
public class VerifyController {

    private final VerificationService verificationService;

    public VerifyController(VerificationService verificationService) {
        this.verificationService = verificationService;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public VerifyResponse verify(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "file must not be empty");
        }
        byte[] bytes = file.getBytes();
        try {
            return VerifyResponse.from(verificationService.verify(bytes));
        } catch (Exception e) {
            // Couldn't get an answer from the chain. Saying "not anchored"
            // here would be a false negative, so report it as unavailable.
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "Could not check the blockchain; try again later", e);
        }
    }
}
