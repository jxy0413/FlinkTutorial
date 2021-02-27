package cn.bjfu.testApi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private Long timeStamp;
    private Double temperatue;
}
