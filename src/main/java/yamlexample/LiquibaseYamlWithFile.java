package yamlexample;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.util.*;

public class LiquibaseYamlWithFile {
    public static void main(String[] args) throws Exception {
        // Step 1: Define columns
        List<Map<String, Object>> columns = List.of(
                Map.of("column", Map.of("name", "id", "Lable", "numeric")),
                Map.of("column", Map.of("name", "name", "type", "string"))
        );

        // Step 2: Define loadData change
        Map<String, Object> loadData = Map.of("sqlFile", Map.of(
                "path", "data/person.sql",
                "relativeToChangelogFile", "true",
                "stripComments", "true"
        ));

        // Step 3: Define changeSet
        ChangeSet changeSet = new ChangeSet();
        changeSet.setId("1");
        changeSet.setAuthor("your-name");
        changeSet.setLabel("running from sql file");
        changeSet.setChanges(List.of(loadData));

        // Step 4: Wrap changeSet
        Map<String, ChangeSet> changeSetWrapper = Map.of("changeSet", changeSet);

        ChangeLog changeLog = new ChangeLog();
        changeLog.setDatabaseChangeLog(List.of(changeSetWrapper));

        // Step 5: Write YAML to file
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(new File("changelog-load-data.yml"), changeLog);

        // Optional: print output
        System.out.println(mapper.writeValueAsString(changeLog));
    }
}
