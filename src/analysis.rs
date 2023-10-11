use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
/// Module that captures how to build and execute an analysis
///
use serde_json::Value;

#[derive(Clone)]
pub struct Analysis {
    inner: Arc<AnalysisRef>,
}

/// A `AnalysisBuilder` used to create a `Analysis`
#[must_use]
pub struct AnalysisBuilder {
    config: Config,
}
impl Default for AnalysisBuilder {
    fn default() -> Self {
        Self::new()
    }
}

struct Config {
    // update fmt::Display
    error: Option<Error>,
    data_source: Option<DataSource>,
    settings: Option<Settings>,
}

impl AnalysisBuilder {
    pub fn new() -> AnalysisBuilder {
        AnalysisBuilder {
            config: Config {
                error: None,
                data_source: None,
                settings: None,
            },
        }
    }
    /// builder, build, then run
    pub fn build(self) -> Result<Analysis> {
        let config = self.config;

        if let Some(err) = config.error {
            return Err(err);
        }

        let computation = |input| {
            // can generate an error
            let result = Computation::new(input);
            match result {
                Ok(output) => Ok(Some(output)),
                Err(err) => Ok(None),
            }
        };

        // get the input into memory?
        let data = if let Some(source) = config.data_source {
            source.get().map_err(crate::error::builder)
        } else {
            return Err(crate::error::builder("Missing data source"));
        };

        Ok(Analysis {
            inner: Arc::new(AnalysisRef {
                input: todo!(),
                output: todo!(),
                computation: todo!(),
                completed: todo!(),
                maybe_value: todo!(),
            }),
        })
    }
}

//------------------------------------------------------------------------------------------------
// AnalysisRef
//------------------------------------------------------------------------------------------------
use std::fmt;
struct AnalysisRef<F, Out>
where
    F: Fn(DataSource) -> Out,
{
    input: DataSource,
    computation: F,
    output: Out,
    completed: bool,
    maybe_value: Option<String>,
}
impl<F, Out> AnalysisRef<F, Out> {
    fn fmt_fields(&self, f: &mut fmt::DebugStruct<'_, '_>) {
        // Instead of deriving Debug, only print fields when their output
        // would provide relevant or interesting data.
        if self.completed {
            f.field("completed", &true);
        }

        f.field("input", &self.input);

        if let Some(ref d) = self.maybe_value {
            f.field("maybe value", d);
        }
    }
}
//------------------------------------------------------------------------------------------------
// WIP meta structures
//------------------------------------------------------------------------------------------------
pub struct Computation {}
impl Computation {
    pub fn new<T>(input: T) -> Result<Self> {
        Ok(Computation {})
    }
}
pub struct Settings {
    inner: Value,
}

#[derive(Debug)]
pub enum DataSource {
    Buffer,
    File(Path),
}

impl DataSource {
    fn get<T>(self) -> Result<Box<T>> {
        match self {
            Buffer => todo!(),
            File => todo!(),
        }
    }
}
