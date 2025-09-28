package com.example.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record InsertOneRequest(
    @NotBlank String messageDatetime,      // "2025-08-11 10:15:30"
    @NotBlank String subPktType,           // "0000"
    @NotNull  Integer stationaryKavachId,  // 50001
    @NotNull  Integer messageSequence      // 5000
) {}
