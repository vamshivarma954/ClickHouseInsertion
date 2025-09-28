package com.example.api;

import com.example.service.ClickHouseInsertService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api")
public class InsertOneController {
  private final ClickHouseInsertService service;
  public InsertOneController(ClickHouseInsertService service) { this.service = service; }

  @PostMapping("/insert-one")
  @ResponseStatus(HttpStatus.CREATED)
  public Mono<Void> insertOne(@Valid @RequestBody InsertOneRequest req) {
    return service.insertOne(req);
  }
}
