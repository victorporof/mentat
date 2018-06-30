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
};
use tx_mapper::{
    TxMapper,
};
use types::{
    Tx,
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
    Merge,
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

impl Syncer {
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

    fn first_sync_against_non_empty(db_tx: &mut rusqlite::Transaction, remote_client: &RemoteClient, local_metadata: &SyncMetadata) -> Result<SyncResult> {
        d(&format!("server non-empty on first sync, adopting remote state."));

        let incoming_txs = remote_client.get_transaction_data_after(&Uuid::nil())?;

        if incoming_txs.len() == 0 {
            bail!(TolstoyError::BadServerState("Server specified non-root HEAD but gave no transactions".to_string()));
        }

        // We assume that the first transaction encountered on first sync is the bootstrap transaction.
        let bootstrap_tx = &incoming_txs[0];
        let remote_bootstrap = BootstrapHelper::new(bootstrap_tx);
        
        if !remote_bootstrap.is_compatible()? {
            bail!(TolstoyError::IncompatibleBootstrapSchema(CORE_SCHEMA_VERSION as i64, remote_bootstrap.core_schema_version()?));
        }

        d(&format!("mapping incoming bootstrap tx uuid to local bootstrap entid: {} -> {}", bootstrap_tx.tx, local_metadata.root));

        // Map incoming bootstrap tx uuid to local bootstrap entid.
        // If there's more work to do, we'll move the head again.
        SyncMetadata::set_remote_head(db_tx, &bootstrap_tx.tx)?;
        TxMapper::set_tx_uuid(db_tx, local_metadata.root, &bootstrap_tx.tx)?;

        // Both remote and local have transactions beyond bootstrap.
        if incoming_txs.len() > 1 && local_metadata.root != local_metadata.head {
            bail!(TolstoyError::NotYetImplemented("Can't purge local state: local excision not supported yet".to_string()));
        }

        Ok(SyncResult::LocalFastForward(incoming_txs[1 ..].to_vec()))
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
                // TODO it's possible that we've successfully advanced remote head previously,
                // but failed to advance our own local head. If that's the case, and we can recognize it,
                // our sync becomes just bumping our local head. AFAICT below would currently fail.
                if let Some(upload_from_tx) = TxMapper::get_tx_for_uuid(db_tx, &locally_known_remote_head)? {
                    d(&format!("Fast-forwarding the server."));
                    Syncer::fast_forward_server(db_tx, Some(upload_from_tx), &remote_client, &remote_head)?;
                    Ok(SyncResult::ServerFastForward)
                } else {
                    d(&format!("Unable to fast-forward the server; missing local tx mapping"));
                    bail!(TolstoyError::TxIncorrectlyMapped(0));
                }
            },

            SyncAction::LocalFastForward => {
                d(&format!("fast-forwarding local store."));
                Ok(SyncResult::LocalFastForward(
                    remote_client.get_transaction_data_after(&locally_known_remote_head)?
                ))
            },

            SyncAction::CombineChanges => {
                Ok(SyncResult::Merge)
            },
        }
    }
}
