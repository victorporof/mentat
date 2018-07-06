// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;

use uuid::Uuid;

use mentat_core::{
    Entid,
    TypedValue,
};

use mentat_db::PartitionMap;

use errors::{
    Result,
};

use tx_mapper::{
    TxMapper,
};

#[derive(Debug, Clone)]
pub enum TxIdentifier {
    Global(Uuid),
    Local(Entid),
}

impl From<Uuid> for TxIdentifier {
    fn from(value: Uuid) -> TxIdentifier {
        TxIdentifier::Global(value)
    }
}

impl TxIdentifier {
    pub fn as_local(&self, db_tx: &rusqlite::Transaction) -> Result<Option<Entid>> {
        match self {
            &TxIdentifier::Global(uuid) => Ok(TxMapper::get_tx_for_uuid(db_tx, &uuid)?),
            &TxIdentifier::Local(e) => Ok(Some(e))
        }
    }

    pub fn as_remote(&self, db_tx: &rusqlite::Transaction) -> Result<Option<Uuid>> {
        match self {
            &TxIdentifier::Global(uuid) => Ok(Some(uuid)),
            &TxIdentifier::Local(e) => Ok(TxMapper::get(db_tx, e)?)
        }
    }
}

pub trait Transactable {
    fn known_entid(&self) -> Option<Entid>;
    fn parts(&self) -> &Vec<TxPart>;
}

// TODO unite these around TxIdentifier?
#[derive(Debug, Clone)]
pub struct LocalTx {
    pub tx: Entid,
    pub parts: Vec<TxPart>,
}

// For returning out of the downloader as an ordered list.
#[derive(Debug, Clone)]
pub struct Tx {
    pub tx: Uuid,
    pub parts: Vec<TxPart>,
}

impl Transactable for LocalTx {
    fn known_entid(&self) -> Option<Entid> {
        Some(self.tx)
    }

    fn parts(&self) -> &Vec<TxPart> {
        &self.parts
    }
}

impl Transactable for Tx {
    fn known_entid(&self) -> Option<Entid> {
        None
    }

    fn parts(&self) -> &Vec<TxPart> {
        &self.parts
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct TxPart {
    // TODO this is a temporary for development. Only first TxPart in a chunk series should have a non-None 'parts'.
    // 'parts' should actually live in a transaction, but we do this now to avoid changing the server until dust settles.
    pub partitions: Option<PartitionMap>,
    pub e: Entid,
    pub a: Entid,
    pub v: TypedValue,
    pub tx: Entid,
    pub added: bool,
}
