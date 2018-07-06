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
};
use mentat_db::{
    CORE_SCHEMA_VERSION,
};
use bootstrap::{
    BootstrapHelper,
};
use errors::{
    TolstoyError,
    Result,
};
use metadata::{
    PartitionsTable,
    SyncMetadata,
};
use remote_client::{
    RemoteClient,
};
use schema::{
    ensure_current_version,
};
use tx_uploader::TxUploader;
use tx_processor::{
    Processor,
    TxReceiver,
};
use tx_mapper::{
    TxMapper,
};
use types::{
    LocalTx,
    Tx,
    TxPart,
};

use logger::d;

pub struct Syncer {}

pub enum SyncResult {
    BadServerState,
    EmptyServer,
    NoChanges,
    ServerFastForward,
    AdoptedRemoteOnFirstSync,
    IncompatibleBootstrapSchema,
    LocalFastForward(Vec<Tx>),
    Merge(Vec<Tx>, Vec<LocalTx>),
}

enum SyncAction {
    NoOp,
    // TODO this is the same as server fast-forward from local root.
    PopulateServer,
    ServerFastForward,
    LocalFastForward,
    // Generic name since we might merge, or rebase, or do something else.
    CombineChanges,
}

// "Changed" is relative to previous sync.
pub enum RemoteDataState {
    Empty,
    Changed,
    Unchanged,
}

// Local client can't be empty: there's always at least a bootstrap transaction.
// "Changed" is relative to previous sync.
pub enum LocalDataState {
    Changed,
    Unchanged,
}

pub struct LocalTxSet {
    pub txs: Vec<LocalTx>,
}

impl LocalTxSet {
    pub fn new() -> LocalTxSet {
        LocalTxSet {
            txs: vec![]
        }
    }
}

impl TxReceiver for LocalTxSet {
    fn tx<T>(&mut self, tx_id: Entid, datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        self.txs.push(LocalTx {
            tx: tx_id,
            parts: datoms.collect()
        });
        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Syncer {
    /// Upload local txs: (from_tx, HEAD]
    fn fast_forward_server(db_tx: &mut rusqlite::Transaction, from_tx: Option<Entid>, remote_client: &RemoteClient, remote_head: &Uuid) -> Result<()> {
        let mut uploader = TxUploader::new(remote_client, remote_head, SyncMetadata::get_partitions(db_tx, PartitionsTable::Core)?);
        Processor::process(db_tx, from_tx, &mut uploader)?;
        if !uploader.is_done {
            bail!(TolstoyError::TxProcessorUnfinished);
        }
        // Last tx uuid uploaded by the tx receiver.
        // It's going to be our new head.
        if let Some(last_tx_uploaded) = uploader.rolling_temp_head {
            // Upload remote head.
            remote_client.put_head(&last_tx_uploaded)?;

            // On success:
            // - persist local mappings from the receiver
            // - update our local "remote head".
            TxMapper::set_bulk(db_tx, &uploader.tx_temp_uuids)?;
            SyncMetadata::set_remote_head(db_tx, &last_tx_uploaded)?;
        }

        Ok(())
    }

    fn what_do(remote_state: RemoteDataState, local_state: LocalDataState) -> SyncAction {
        match remote_state {
            RemoteDataState::Empty => {
                SyncAction::PopulateServer
            },

            RemoteDataState::Changed => {
                match local_state {
                    LocalDataState::Changed => {
                        SyncAction::CombineChanges
                    },

                    LocalDataState::Unchanged => {
                        SyncAction::LocalFastForward
                    },
                }
            },

            RemoteDataState::Unchanged => {
                match local_state {
                    LocalDataState::Changed => {
                        SyncAction::ServerFastForward
                    },

                    LocalDataState::Unchanged => {
                        SyncAction::NoOp
                    },
                }
            },
        }
    }

    fn remote_data_state(known_remote_head: &Uuid, actual_remote_head: &Uuid) -> RemoteDataState {
        if *actual_remote_head == Uuid::nil() {
            RemoteDataState::Empty

        } else if actual_remote_head != known_remote_head {
            RemoteDataState::Changed
        } else {
            RemoteDataState::Unchanged
        }
    }

    fn local_data_state(db_tx: &rusqlite::Transaction, local_metadata: &SyncMetadata) -> Result<LocalDataState> {
        // We have local changes if head transaction hasn't been "mapped" (i.e. uploaded).
        if TxMapper::get(db_tx, local_metadata.head)?.is_none() {
            Ok(LocalDataState::Changed)
        } else {
            Ok(LocalDataState::Unchanged)
        }
    }

    /// Unsafe and unaware brain surgery!
    pub fn rewind_tx(db_tx: &rusqlite::Transaction, tx_id: Entid) -> Result<()> {
        // Delete additions made in tx.
        db_tx.execute("DELETE FROM datoms WHERE rowid IN (SELECT d.rowid FROM transactions AS t, datoms AS d
                WHERE
                    t.tx = ? AND
                    t.e = d.e AND
                    t.a = d.a AND
                    t.value_type_tag = d.value_type_tag AND
                    t.v = d.v)",
                            &[&tx_id])?;
        // Insert retractions made in tx.
        db_tx.execute("INSERT INTO datoms (e, a, v, tx, value_type_tag) SELECT t.e, t.a, t.v, ?, t.value_type_tag FROM transactions AS t WHERE t.tx = ? AND t.added = 0", &[&tx_id, &tx_id])?;
        Ok(())
    }

    pub fn eradicate_from_transactions(db_tx: &rusqlite::Transaction, tx_id: Entid) -> Result<()> {
        db_tx.execute("DELETE FROM transactions WHERE tx = ?", &[&tx_id])?;
        Ok(())
    }
    
    fn map_remote_bootstrap(db_tx: &mut rusqlite::Transaction, remote_bootstrap: &Tx, local_bootstrap: Entid) -> Result<()> {
        let helper = BootstrapHelper::new(remote_bootstrap);

        if !helper.is_compatible()? {
            bail!(TolstoyError::IncompatibleBootstrapSchema(CORE_SCHEMA_VERSION as i64, helper.core_schema_version()?));
        }

        d(&format!("mapping incoming bootstrap tx uuid to local bootstrap entid: {} -> {}", remote_bootstrap.tx, local_bootstrap));

        // Map incoming bootstrap tx uuid to local bootstrap entid.
        // If there's more work to do, we'll move the head again.
        SyncMetadata::set_remote_head(db_tx, &remote_bootstrap.tx)?;
        TxMapper::set_tx_uuid(db_tx, local_bootstrap, &remote_bootstrap.tx)?;

        Ok(())
    }

    fn first_sync_against_non_empty(db_tx: &mut rusqlite::Transaction, remote_client: &RemoteClient, local_metadata: &SyncMetadata) -> Result<SyncResult> {
        d(&format!("server non-empty on first sync, adopting remote state."));

        // 1) Download remote transactions.
        let incoming_txs = remote_client.get_transaction_data_after(&Uuid::nil())?;
        if incoming_txs.len() == 0 {
            bail!(TolstoyError::BadServerState("Server specified non-root HEAD but gave no transactions".to_string()));
        }

        // 2) Process remote bootstrap.
        Syncer::map_remote_bootstrap(db_tx, &incoming_txs[0], local_metadata.root)?;

        // 3) Determine new local and remote data states, now that bootstrap has been dealt with.
        let remote_state = if incoming_txs.len() > 1 {
            RemoteDataState::Changed
        } else {
            RemoteDataState::Unchanged
        };

        let local_state = if local_metadata.root != local_metadata.head {
            LocalDataState::Changed
        } else {
            LocalDataState::Unchanged
        };

        // 4) The rest isn't special anymore: proceed with regular flow. TODO merge two flows together?
        // This is largely the same as the main flow, except we already have a set of incoming transactions.

        match Syncer::what_do(remote_state, local_state) {
            SyncAction::NoOp => {
                Ok(SyncResult::NoChanges)
            },

            SyncAction::PopulateServer => {
                bail!(TolstoyError::UnexpectedState(format!("Remote state can't be empty on first sync against non-empty server")))
            },

            SyncAction::ServerFastForward => {
                bail!(TolstoyError::NotYetImplemented(format!("TODO fast-forward server on first sync against non-empty when remote is just bootstrap and local has more")))
            },

            SyncAction::LocalFastForward => {
                Ok(SyncResult::LocalFastForward(incoming_txs[1 ..].to_vec()))
            },

            SyncAction::CombineChanges => {
                let mut local_txs = LocalTxSet::new();
                Processor::process(db_tx, Some(local_metadata.root), &mut local_txs)?;
                Ok(SyncResult::Merge(incoming_txs[1 ..].to_vec(), local_txs.txs))
            }
        }
    }

    fn local_tx_for_uuid(db_tx: &rusqlite::Transaction, uuid: &Uuid) -> Result<Entid> {
        match TxMapper::get_tx_for_uuid(db_tx, uuid)? {
            Some(t) => Ok(t),
            None => bail!(TolstoyError::TxIncorrectlyMapped(0))
        }
    }

    pub fn flow(db_tx: &mut rusqlite::Transaction, server_uri: &String, user_uuid: &Uuid) -> Result<SyncResult> {
        d(&format!("sync flowing"));

        ensure_current_version(db_tx)?;
        
        // TODO auth, crypto, etc...
        let remote_client = RemoteClient::new(server_uri.clone(), user_uuid.clone());

        let remote_head = remote_client.get_head()?;
        d(&format!("remote head {:?}", remote_head));

        let locally_known_remote_head = SyncMetadata::remote_head(db_tx)?;
        d(&format!("local head {:?}", locally_known_remote_head));

        let (root, head) = SyncMetadata::root_and_head_tx(db_tx)?;
        let local_metadata = SyncMetadata::new(root, head);
        let local_state = Syncer::local_data_state(db_tx, &local_metadata)?;
        let remote_state = Syncer::remote_data_state(&locally_known_remote_head, &remote_head);

        // Currently, first sync against a non-empty server is special.
        if locally_known_remote_head == Uuid::nil() && remote_head != Uuid::nil() {
            return Syncer::first_sync_against_non_empty(db_tx, &remote_client, &local_metadata);
        }

        match Syncer::what_do(remote_state, local_state) {
            SyncAction::NoOp => {
                d(&format!("local HEAD did not move. Nothing to do!"));
                Ok(SyncResult::NoChanges)
            },

            SyncAction::PopulateServer => {
                d(&format!("empty server!"));
                Syncer::fast_forward_server(db_tx, None, &remote_client, &remote_head)?;
                Ok(SyncResult::EmptyServer)
            },

            SyncAction::ServerFastForward => {
                d(&format!("local HEAD moved."));
                let upload_from_tx = Syncer::local_tx_for_uuid(db_tx, &locally_known_remote_head)?;

                d(&format!("Fast-forwarding the server."));

                // TODO it's possible that we've successfully advanced remote head previously,
                // but failed to advance our own local head. If that's the case, and we can recognize it,
                // our sync becomes just bumping our local head. AFAICT below would currently fail.
                Syncer::fast_forward_server(db_tx, Some(upload_from_tx), &remote_client, &remote_head)?;
                Ok(SyncResult::ServerFastForward)
            },

            SyncAction::LocalFastForward => {
                d(&format!("fast-forwarding local store."));
                Ok(SyncResult::LocalFastForward(
                    remote_client.get_transaction_data_after(&locally_known_remote_head)?
                ))
            },

            SyncAction::CombineChanges => {
                d(&format!("combining changes from local and remote stores."));
                let mut local_txs = LocalTxSet::new();
                Processor::process(db_tx, Some(Syncer::local_tx_for_uuid(db_tx, &locally_known_remote_head)?), &mut local_txs)?;
                Ok(SyncResult::Merge(
                    remote_client.get_transaction_data_after(&locally_known_remote_head)?,
                    local_txs.txs
                ))
            },
        }
    }
}
