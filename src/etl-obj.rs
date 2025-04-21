use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fmt;

/// impl fmt::Display using a summary version of EtlObject that uses the Debug implementations
/// and the fmt::Display implementations of the EtlField and EtlUnit enums.
impl fmt::Display for EtlObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EtlObject {{ etl_fields: {{\n{}\n}}, etl_units: {{\n{}\n}} }}",
            self.etl_fields
                .iter()
                .map(|(name, etl_field)| format!("{}: {}", name, etl_field))
                .collect::<Vec<String>>()
                .join(",\n"),
            self.etl_units
                .iter()
                .map(|(name, etl_unit)| format!("{}: {}", name, etl_unit))
                .collect::<Vec<String>>()
                .join(",\n")
        )
    }
}

/// EtlObject
/// Todo: Consider where I'm getting the details of the information.
/// Specifically, the levels data.
/// Todo: Update the types for codomain used in the EtlUnit, EtlField vs Source contexts.
///
#[derive(Debug, Deserialize, Serialize)]
pub struct EtlObject {
    #[serde(rename = "etlFields")]
    pub etl_fields: HashMap<String, EtlField>,
    #[serde(rename = "etlUnits")]
    pub etl_units: HashMap<String, EtlUnit>,
}

/// implement fmt::Display for  EtlUnit, show the enum variant, the codomain,
/// and for the Measurement variant, the mcomps count and mspan name. Do not
/// include the codomain_reducer. Format the output to be more readable.
impl fmt::Display for EtlUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EtlUnit::Quality(quality) => {
                write!(f, "EtlUnit::Quality {{ codomain: {} }}", quality.codomain)
            }
            EtlUnit::Measurement(measurement) => {
                write!(
                    f,
                    "EtlUnit::Measurement {{ codomain: {}, mcomps: {}, mspan: {} }}",
                    measurement.codomain,
                    measurement.mcomps.len(),
                    measurement.mspan
                )
            }
            EtlUnit::Subject(subject) => {
                write!(f, "EtlUnit::Subject {{ codomain: {} }}", subject.codomain)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum EtlUnit {
    #[serde(rename = "quality")]
    Quality(EtlUnitQuality),
    #[serde(rename = "mvalue")]
    Measurement(EtlUnitMeasurement),
    #[serde(rename = "subject")]
    Subject(EtlUnitSubject),
}

pub type Name = String;

/// The codomain is the namesake for the EtlUnit. Each name references a EtlField.
#[derive(Debug, Deserialize, Serialize)]
pub struct EtlUnitQuality {
    pub subject: Name,
    pub codomain: Name,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: Reducer,
}

/// The codomain is the namesake for the EtlUnit. Each name references a EtlField.
#[derive(Debug, Deserialize, Serialize)]
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
// EtlUnitSubject
#[derive(Debug, Deserialize, Serialize)]
pub struct EtlUnitSubject {
    pub subject: Name,
    pub codomain: Name,
}

/// Enum to represent different kinds of EtlFields based on the purpose
#[derive(Debug, Deserialize, Serialize)]
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
/// implement fmt::Display for  EtlField
/// such that we show the enum variant, name, etl_unit, the number of sources and the sources
impl fmt::Display for EtlField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EtlField::Subject(subject) => {
                write!(
                    f,
                    "EtlField::Subject {{ name: {}, etl_unit: None, source count: {}, sources: {} }}",
                    subject.name,
                    subject.sources.len(),
                    subject.sources
                        .iter()
                        .map(|src| format!("{}", src))
                        .collect::<Vec<String>>()
                        .join(",\n")
                        )
            }
            EtlField::Quality(quality) => {
                write!(
                    f,
                    "EtlField::Quality {{ name: {}, etl_unit: {}, source count: {}, sources: {} }}",
                    quality.name,
                    quality
                        .etl_unit
                        .iter()
                        .map(|unit| unit.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    quality.sources.len(),
                    quality
                        .sources
                        .iter()
                        .map(|src| format!("{}", src))
                        .collect::<Vec<String>>()
                        .join(",\n")
                )
            }
            EtlField::MComp(mcomp) => {
                write!(
                    f,
                    "EtlField::MComp {{ name: {}, etl_unit: {}, source count: {} sources: {} }}",
                    mcomp.name,
                    mcomp
                        .etl_unit
                        .iter()
                        .map(|unit| unit.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    mcomp.sources.len(),
                    mcomp
                        .sources
                        .iter()
                        .map(|src| format!("{}", src))
                        .collect::<Vec<String>>()
                        .join(",\n")
                )
            }
            EtlField::MSpan(mspan) => {
                write!(
                    f,
                    "EtlField::MSpan {{ name: {}, etl_unit: {}, source count: {}, sources: {} }}",
                    mspan.name,
                    mspan
                        .etl_unit
                        .iter()
                        .map(|unit| unit.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    mspan.sources.len(),
                    mspan
                        .sources
                        .iter()
                        .map(|src| format!("{}", src))
                        .collect::<Vec<String>>()
                        .join(",\n")
                )
            }
            EtlField::MValue(mvalue) => {
                write!(
                    f,
                    "EtlField::MValue {{ name: {}, etl_unit: {}, source count: {}, sources: {} }}",
                    mvalue.name,
                    mvalue
                        .etl_unit
                        .iter()
                        .map(|unit| unit.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    mvalue.sources.len(),
                    mvalue
                        .sources
                        .iter()
                        .map(|src| format!("{}", src))
                        .collect::<Vec<String>>()
                        .join(",\n")
                )
            }
        }
    }
}

/// Structs for each kind of EtlField (see enum). They all have a sources property.
#[derive(Debug, Deserialize, Serialize)]
pub struct SubjectField {
    pub idx: u32,
    pub name: Name,
    pub format: Option<String>,
    pub sources: Vec<Source>,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct QualityField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Vec<Name>,
    pub format: Option<String>,
    pub null_value_expansion: Option<String>,
    #[serde(rename = "map-weights")]
    pub map_weights: MapWeights,
    pub map_files: Option<HashMap<String, String>>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MCompField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Vec<Name>,
    pub format: Option<String>,
    #[serde(rename = "map-weights")]
    pub map_weights: MapWeights,
    pub map_files: Option<HashMap<String, String>>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MSpanField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Vec<Name>,
    pub format: Option<String>,
    pub time: Time,
    #[serde(rename = "levels-mspan")]
    pub levels_mspan: Vec<Range>,
    pub sources: Vec<Source>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MValueField {
    pub idx: u32,
    pub name: Name,
    #[serde(rename = "etl-unit")]
    pub etl_unit: Vec<Name>,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct MapSymbols {
    #[serde(rename = "arrows")]
    pub arrows: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LevelsMspan {
    #[serde(rename = "rangeStart")]
    pub range_start: i64,
    #[serde(rename = "rangeLength")]
    pub range_length: i64,
    pub reduced: bool,
}

// struct MapImplied so that it can host either u32 or a String
#[derive(Debug, Deserialize, Serialize)]
pub struct MapImplied {
    pub domain: String,
    pub codomain: Codomain,
}
#[derive(Debug, Serialize)]
pub enum Codomain {
    Number(u32),
    Text(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MapWeights {
    #[serde(rename = "arrows")]
    pub arrows: HashMap<String, f32>,
}

// Time
#[derive(Debug, Deserialize, Serialize)]
pub struct Range {
    #[serde(rename = "rangeStart")]
    pub range_start: u32,
    #[serde(rename = "rangeLength")]
    pub range_length: u32,
    pub reduced: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Time {
    pub interval: Interval,
    pub reference: Reference,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct Interval {
    pub unit: String,
    pub count: u32,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct Reference {
    pub idx: u32,
    pub value: String,
    #[serde(rename = "isoFormat")]
    pub iso_format: String,
}

pub type Filename = String;

#[derive(Debug, Deserialize, Serialize)]
pub struct MapFiles {
    #[serde(rename = "arrows")]
    pub arrows: HashMap<Filename, String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Level {
    pub count: u32,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub enum Reducer {
    FIRST,
    LAST,
    AVG,
    SUM,
    MIN,
    MAX,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "source-type")]
pub enum Source {
    #[serde(rename = "RAW")]
    Raw(SourceRaw),
    #[serde(rename = "IMPLIED")]
    Implied(SourceImplied),
    #[serde(rename = "WIDE")]
    Wide(SourceWide),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SourceRaw {
    pub enabled: bool,
    #[serde(rename = "header-idx")]
    pub header_idx: u32,
    #[serde(rename = "header-name")]
    pub header_name: String,
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

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
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

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Source::Raw(raw) => {
                write!(
                    f,
                    "Source::Raw {{ nlevels: {}, nrows: {}, filename: {}, header-idx: {} }}",
                    raw.nlevels, raw.nrows, raw.filename, raw.header_idx
                )
            }
            Source::Implied(implied) => {
                write!(
                    f,
                    "Source::Implied {{ nlevels: {}, filename: {} }}",
                    implied.nlevels, implied.filename
                )
            }
            Source::Wide(wide) => {
                write!(
                    f,
                    "Source::Wide {{ nlevels: {}, nrows: {}, filename: {}, header-idx: {} }}",
                    wide.nlevels, wide.nrows, wide.filename, wide.header_idx
                )
            }
        }
    }
}

impl<'de> Deserialize<'de> for Codomain {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CodomainVisitor;

        impl<'de> Visitor<'de> for CodomainVisitor {
            type Value = Codomain;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a number or a string")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Codomain, E>
            where
                E: de::Error,
            {
                Ok(Codomain::Number(value as u32))
            }

            fn visit_str<E>(self, value: &str) -> Result<Codomain, E>
            where
                E: de::Error,
            {
                Ok(Codomain::Text(value.to_owned()))
            }
        }

        deserializer.deserialize_any(CodomainVisitor)
    }
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
                "NPI Number": {
                  "type": "subject",
                  "subject": "NPI Number",
                  "codomain": "NPI Number",
                  "codomain-reducer": null
                },
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

        assert!(etl_object.etl_fields.contains_key("in network"));
    }
}
