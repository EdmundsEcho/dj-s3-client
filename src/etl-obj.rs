use serde::Deserialize;
use serde_json;
use std::collections::HashMap;

/// Next: Consider where I'm getting the details of the information.
/// Specifically, the levels data.
///
/// Next: EtlFieldKind -> EtlField with inner
///

/// Write a data structure that can be deserialized using the
/// json found in /Users/edmund/Downloads/etlObj.json
#[derive(Debug, Deserialize)]
pub struct EtlObject {
    #[serde(rename = "etlFields")]
    pub etl_fields: HashMap<String, EtlField>,
    #[serde(rename = "etlUnits")]
    pub etl_units: HashMap<String, EtlUnit>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum EtlUnit {
    #[serde(rename = "quality")]
    Quality(EtlUnitQuality),
    #[serde(rename = "mvalue")]
    Measurement(EtlUnitMeasurement),
}

pub type Name = String;

#[derive(Debug, Deserialize)]
pub struct EtlUnitQuality {
    pub subject: Name,
    pub codomain: Name,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Reducer,
}

#[derive(Debug, Deserialize)]
pub struct EtlUnitMeasurement {
    pub subject: Name,
    pub codomain: Name,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Reducer,
    pub mcomps: Vec<Name>,
    pub mspan: Name,
    #[serde(rename = "slicing-reducer")]
    pub slicing_reducer: Reducer,
}

/// Enum to represent different kinds of EtlFields based on the purpose
#[derive(Debug, Deserialize)]
#[serde(tag = "purpose")]
pub enum EtlField {
    #[serde(rename = "subject")]
    Subject(SubjectField),
    #[serde(rename = "quality")]
    Quality(QualityField),
    #[serde(rename = "mcomp")]
    MComp(MCompField),
    #[serde(rename = "mspan")]
    MSpan(MSpanField),
    #[serde(rename = "mvalue")]
    MValue(MValueField),
}

/// Structs for each kind of EtlField (see enum). They all have a sources property.
#[derive(Debug, Deserialize)]
pub struct SubjectField {
    pub idx: u32,
    pub name: Name,
    pub format: Option<String>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize)]
pub struct QualityField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Name,
    pub format: Option<String>,
    pub null_value_expansion: Option<String>,
    #[serde(rename = "map-weights")]
    pub map_weights: MapWeights,
    pub map_files: Option<HashMap<String, String>>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize)]
pub struct MCompField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Name,
    pub format: Option<String>,
    #[serde(rename = "map-weights")]
    pub map_weights: MapWeights,
    pub map_files: Option<HashMap<String, String>>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize)]
pub struct MSpanField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Name,
    pub format: Option<String>,
    pub time: Time,
    #[serde(rename = "levels-mspan")]
    pub levels_mspan: Vec<Range>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize)]
pub struct MValueField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Name,
    pub format: Option<String>,
    #[serde(rename = "null-value-expansion")]
    pub null_value_expansion: Option<String>,
    #[serde(rename = "map-files")]
    pub map_files: Option<HashMap<String, String>>,
    #[serde(rename = "map-weights")]
    pub map_weights: Option<HashMap<String, HashMap<String, i32>>>,
    #[serde(rename = "map-symbols")]
    pub map_symbols: HashMap<String, HashMap<String, String>>,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Reducer,
    #[serde(rename = "slicing-reducer")]
    pub slicing_reducer: Reducer,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize)]
pub struct MapSymbols {
    #[serde(rename = "arrows")]
    pub arrows: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct LevelsMspan {
    #[serde(rename = "rangeStart")]
    pub range_start: i64,
    #[serde(rename = "rangeLength")]
    pub range_length: i64,
    pub reduced: bool,
}

#[derive(Debug, Deserialize)]
pub struct MapImplied {
    pub domain: String,
    pub codomain: u32,
}

#[derive(Debug, Deserialize)]
pub struct MapWeights {
    #[serde(rename = "arrows")]
    pub arrows: HashMap<String, f32>,
}

// Time
#[derive(Debug, Deserialize)]
pub struct Range {
    #[serde(rename = "rangeStart")]
    pub range_start: u32,
    #[serde(rename = "rangeLength")]
    pub range_length: u32,
    pub reduced: bool,
}

#[derive(Debug, Deserialize)]
pub struct Time {
    pub interval: Interval,
    pub reference: Reference,
}
#[derive(Debug, Deserialize)]
pub struct Interval {
    pub unit: String,
    pub count: u32,
}
#[derive(Debug, Deserialize)]
pub struct Reference {
    pub idx: u32,
    pub value: String,
    #[serde(rename = "isoFormat")]
    pub iso_format: String,
}

pub type Filename = String;

#[derive(Debug, Deserialize)]
pub struct MapFiles {
    #[serde(rename = "arrows")]
    pub arrows: HashMap<Filename, String>,
}

#[derive(Debug, Deserialize)]
pub struct Level {
    pub count: u32,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub enum Purpose {
    #[serde(rename = "subject")]
    SUBJECT,
    #[serde(rename = "quality")]
    QUALITY,
    #[serde(rename = "mcomp")]
    MCOMP,
    #[serde(rename = "mspan")]
    MSPAN,
    #[serde(rename = "mvalue")]
    MVALUE,
}

#[derive(Debug, Deserialize)]
pub enum Reducer {
    FIRST,
    LAST,
    AVG,
    SUM,
    MIN,
    MAX,
}
#[derive(Debug, Deserialize)]
#[serde(tag = "source-type")]
pub enum Source {
    #[serde(rename = "RAW")]
    Raw(SourceRaw),
    #[serde(rename = "IMPLIED")]
    Implied(SourceImplied),
    #[serde(rename = "WIDE")]
    Wide(SourceWide),
}

#[derive(Debug, Deserialize)]
pub struct SourceRaw {
    pub enabled: bool,
    #[serde(rename = "header-idx")]
    pub header_idx: u32,
    #[serde(rename = "default-name")]
    pub default_name: String,
    #[serde(rename = "field-alias")]
    pub field_alias: String,
    pub purpose: Purpose,
    pub null_value: Option<serde_json::Value>,
    pub format: Option<String>,
    #[serde(rename = "map-symbols")]
    pub map_symbols: MapSymbols,
    pub nlevels: u32,
    pub nrows: u32,
    pub filename: String,
    #[serde(rename = "null-value-count")]
    pub null_value_count: u32,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Option<Reducer>,
    #[serde(rename = "map-weights")]
    pub map_weights: Option<MapWeights>,
}

#[derive(Debug, Deserialize)]
pub struct SourceImplied {
    pub enabled: bool,
    #[serde(rename = "field-alias")]
    pub field_alias: String,
    pub purpose: Purpose,
    pub null_value: Option<serde_json::Value>,
    pub format: Option<String>,
    pub nlevels: u32, // constant 2
    pub filename: String,
    #[serde(rename = "map-implied")]
    pub map_implied: MapImplied,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Option<Reducer>,
    #[serde(rename = "slicing-reducer")]
    pub slicing_reducer: Option<Reducer>,
    #[serde(rename = "map-weights")]
    pub map_weights: Option<MapWeights>,
}

#[derive(Debug, Deserialize)]
pub struct SourceWide {
    pub enabled: bool,
    #[serde(rename = "header-idx")]
    pub header_idx: u32,
    #[serde(rename = "default-name")]
    pub default_name: String,
    #[serde(rename = "field-alias")]
    pub field_alias: String,
    pub purpose: Purpose,
    pub null_value: Option<serde_json::Value>,
    pub format: Option<String>,
    #[serde(rename = "map-symbols")]
    pub map_symbols: MapSymbols,
    pub nlevels: u32,
    pub nrows: u32,
    pub filename: String,
    #[serde(rename = "null-value-count")]
    pub null_value_count: u32,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Option<Reducer>,
    #[serde(rename = "map-weights")]
    pub map_weights: Option<MapWeights>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_etl_object_deserialization() {
        let json_str = r#"
            {
              "etlFields": {
                "in network": {
                  "map-weights": {
                    "arrows": {}
                  },
                  "idx": 1,
                  "name": "in network",
                  "purpose": "quality",
                  "map-symbols": {
                    "arrows": {}
                  },
                  "etl-unit": "in network",
                  "format": null,
                  "null-value-expansion": "0",
                  "map-files": null,
                  "sources": [
                    {
                      "enabled": true,
                      "source-type": "RAW",
                      "header-idx": 7,
                      "default-name": "in network",
                      "field-alias": "in network",
                      "purpose": "quality",
                      "null-value": null,
                      "format": null,
                      "map-symbols": {
                        "arrows": {}
                      },
                      "nlevels": 2,
                      "nrows": 52418,
                      "filename": "/shared/datafiles/.../target_list.csv",
                      "null-value-count": 0,
                      "codomain-reducer": "FIRST",
                      "map-weights": {
                        "arrows": {}
                      }
                    }
                  ],
                  "codomain-reducer": "FIRST"
                }
              },
              "etlUnits": {
                "in network": {
                  "type": "quality",
                  "subject": "npi",
                  "codomain": "in network",
                  "codomain-reducer": "FIRST"
                }
              }
            }
        "#;

        let etl_object: EtlObject = serde_json::from_str(json_str).unwrap();

        assert_eq!(etl_object.etl_fields.contains_key("in network"), true);
    }
}
