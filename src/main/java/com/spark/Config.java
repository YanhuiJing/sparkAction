package com.spark;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Data
public class Config {

  @Value("${appNameAlarm}")
  public String appName;

}
