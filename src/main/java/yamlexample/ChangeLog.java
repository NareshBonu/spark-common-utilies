package yamlexample;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ChangeLog {

    private List<Map<String, ChangeSet>> databaseChangeLog ;
}
