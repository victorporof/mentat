// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    Keyword,
};

use mentat_db::{
    CORE_SCHEMA_VERSION,
};

use errors::{
    Result,
    TolstoyError,
};
use parts::{
    PartsHelper,
};
use types::{
    Tx,
};

pub struct BootstrapHelper<'a> {
    parts: PartsHelper<'a>
}

impl<'a> BootstrapHelper<'a> {
    pub fn new(assumed_bootstrap_tx: &Tx) -> BootstrapHelper {
        BootstrapHelper {
            parts: PartsHelper::new(&assumed_bootstrap_tx.parts),
        }
    }

    // TODO we could also iterate through our own bootstrap schema definition and check that everything matches
    // "version" is used here as a proxy for doing that work
    pub fn is_compatible(&self) -> Result<bool> {
        Ok(self.core_schema_version()? == CORE_SCHEMA_VERSION as i64)
    }

    pub fn core_schema_version(&self) -> Result<i64> {
        match self.parts.ea_lookup(
            Keyword::namespaced("db.schema", "core"),
            Keyword::namespaced("db.schema", "version"),
        ) {
            Some(v) => {
                // TODO v is just a type tag and a Copy value, we shouldn't need to clone.
                match v.clone().into_long() {
                    Some(v) => Ok(v),
                    None => bail!(TolstoyError::BadServerState("incorrect type for core schema version".to_string()))
                }
            },
            None => bail!(TolstoyError::BadServerState("missing core schema version".to_string()))
        }
    }
}
