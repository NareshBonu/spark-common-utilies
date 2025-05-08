package yamlexample;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ChangeSet {

    private String id;
    private String author;
    private String label;
    private List<Map<String, Object>> changes;
}
