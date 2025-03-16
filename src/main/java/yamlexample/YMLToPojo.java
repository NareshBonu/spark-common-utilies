package yamlexample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class YMLToPojo {

    public YMLToPojo() throws IOException {
    }

    public static void main(String[] args) throws Exception {


        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        EnvironmentConfig config = objectMapper.readValue(new File("src/main/resources/config.yaml"), EnvironmentConfig.class);

        Map<String, EnvironmentDetails> environments = config.getEnvironments();
        EnvironmentDetails environmentDetails = environments.get("dev") ;
        System.out.println(environmentDetails.getBucketName());

    }
}
