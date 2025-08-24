package com.indiabulls.shortagedelivery.notification.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShortageEmailTemplateData {
    @JsonProperty("CLIENT")
    private String CLIENT;

    @JsonProperty("ISIN")
    private String ISIN;

    @JsonProperty("QTY")
    private Integer QTY;

    @JsonProperty("PRICE")
    private Double PRICE;
}
