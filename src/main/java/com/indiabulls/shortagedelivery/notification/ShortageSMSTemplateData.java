package com.indiabulls.shortagedelivery.notification;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShortageSMSTemplateData {
    @JsonProperty("CLIENT")
    private String clientId;

    @JsonProperty("ISIN")
    private String isin;

    @JsonProperty("QTY")
    private Integer shortageQty;
}
