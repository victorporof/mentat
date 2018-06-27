// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use uuid::Uuid;

use mentat_core::{
    Entid,
    TypedValue,
};

// For returning out of the downloader as an ordered list.
#[derive(Debug, Clone)]
pub struct Tx {
    pub tx: Uuid,
    pub parts: Vec<TxPart>,
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct TxPart {
    // TODO this is a temporary for development. Only first TxPart in a chunk series should have a non-None 'parts'.
    // 'parts' should actually live in a transaction, but we do this now to avoid changing the server until dust settles.
    pub parts: Option<PartitionMap>,
    pub e: Entid,
    pub a: Entid,
    pub v: TypedValue,
    pub tx: Entid,
    pub added: bool,
}
