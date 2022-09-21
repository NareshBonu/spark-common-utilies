package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class FieldMetadata {


    @JsonProperty("fieldPosition")
    public  int fieldPosition;

    @JsonProperty("fieldName")
    public  String fieldName;

    @JsonProperty("fieldDataType")
    public  String fieldDataType;

    @JsonProperty("fieldLength")
    public  int  fieldLength;

    @JsonProperty("fieldNullability")
    public  boolean  fieldNullability;

    @JsonProperty("fieldPrecision")
    public  int  fieldPrecision;

    @JsonProperty("fieldScale")
    public  int  fieldScale;

    @JsonProperty("fieldDateFormat")
    public  String fieldDateFormat;
}
