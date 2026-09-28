package com.hashanchor.api;

import com.hashanchor.domain.RecordService;
import java.io.IOException;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

/**
 * {@code @RestController} marks this a Spring MVC controller whose method
 * return values are written straight to the HTTP response body (as JSON, by
 * default) rather than resolved to a view template.
 */
@RestController
@RequestMapping("/api/records")
public class RecordController {

    private final RecordService recordService;

    public RecordController(RecordService recordService) {
        this.recordService = recordService;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<RecordResponse> submit(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "file must not be empty");
        }

        var record = recordService.submit(file.getBytes());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(RecordResponse.from(record));
    }

    @GetMapping("/{id}")
    public ResponseEntity<RecordResponse> get(@PathVariable UUID id) {
        return recordService
                .findById(id)
                .map(RecordResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
