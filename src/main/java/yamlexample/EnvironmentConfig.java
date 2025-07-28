package yamlexample;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;


public class EnvironmentConfig {


    private final Map<String, EnvironmentDetails> environments = new HashMap<>();

    @JsonAnySetter
    public void setEnvironment(String name, EnvironmentDetails details) {
        environments.put(name, details);
    }

    public Map<String, EnvironmentDetails> getEnvironments() {
        return environments;
    }

}