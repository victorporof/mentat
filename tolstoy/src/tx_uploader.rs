// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use uuid::Uuid;

use mentat_core::{
    Entid,
};

use mentat_db::{
    USER0,
    TX0,
    Partition,
    PartitionMap,
};

use errors::{
    Result,
};

use schema::{
    PARTITION_USER,
};

use remote_client::{
    RemoteClient,
};

use tx_processor::{
    TxReceiver,
};

use types::{
    TxPart,
};

use logger::d;

pub(crate) struct TxUploader<'c> {
    pub tx_temp_uuids: HashMap<Entid, Uuid>,
    pub is_done: bool,
    remote_client: &'c RemoteClient,
    remote_head: &'c Uuid,
    pub rolling_temp_head: Option<Uuid>,
}

impl<'c> TxUploader<'c> {
    pub fn new(client: &'c RemoteClient, remote_head: &'c Uuid) -> TxUploader<'c> {
        TxUploader {
            tx_temp_uuids: HashMap::new(),
            remote_client: client,
            remote_head: remote_head,
            rolling_temp_head: None,
            is_done: false
        }
    }
}

/// Assumes that user partition's upper bound is the start of the tx partition.
fn within_user_partition(e: Entid) -> bool {
    e >= USER0 && e < TX0
}

impl<'c> TxReceiver for TxUploader<'c> {
    fn tx<T>(&mut self, tx_id: Entid, datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        // Yes, we generate a new UUID for a given Tx, even if we might
        // already have one mapped locally. Pre-existing local mapping will
        // be replaced if this sync succeeds entirely.
        // If we're seeing this tx again, it implies that previous attempt
        // to sync didn't update our local head. Something went wrong last time,
        // and it's unwise to try to re-use these remote tx mappings.
        // We just leave garbage txs to be GC'd on the server.
        let tx_uuid = Uuid::new_v4();
        self.tx_temp_uuids.insert(tx_id, tx_uuid);
        let mut tx_chunks = vec![];

        // TODO separate bits of network work should be combined into single 'future'

        let mut datoms: Vec<TxPart> = datoms.collect();

        // Figure our the "high water-mark" for the user partition.
        let mut largest_e = USER0;
        let mut es_within_user_partition = false;
        for datom in &datoms {
            if within_user_partition(datom.e) && datom.e > largest_e {
                largest_e = datom.e;
                es_within_user_partition = true;
            }
        }

        // Annotate first datom in the series with the user partition information.
        // TODO this is obviously wrong - we want to read partition info without
        // reading/fetching any of the chunks (assertions/retractions)!
        // Partition annotation will move over to Transaction once server support is in place,
        // so this is temporary and for development purposes only.
        let mut tx_partition_map = PartitionMap::default();
        let new_index = if es_within_user_partition {
            largest_e + 1
        } else {
            largest_e
        };
        tx_partition_map.insert(PARTITION_USER.to_string(), Partition::new(USER0, new_index));
        datoms[0].partitions = Some(tx_partition_map);

        // Upload all chunks.
        for datom in &datoms {
            let datom_uuid = Uuid::new_v4();
            tx_chunks.push(datom_uuid);
            d(&format!("putting chunk: {:?}, {:?}", &datom_uuid, &datom));
            // TODO switch over to CBOR once we're past debugging stuff.
            // See https://github.com/mozilla/mentat/issues/570
            // let cbor_val = serde_cbor::to_value(&datom)?;
            // self.remote_client.put_chunk(&datom_uuid, &serde_cbor::ser::to_vec_sd(&cbor_val)?)?;
            self.remote_client.put_chunk(&datom_uuid, &datom)?;
        }

        // Upload tx.
        // NB: At this point, we may choose to update remote & local heads.
        // Depending on how much we're uploading, and how unreliable our connection
        // is, this might be a good thing to do to ensure we make at least some progress.
        // Comes at a cost of possibly increasing racing against other clients.
        match self.rolling_temp_head {
            Some(parent) => {
                d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &parent, &tx_chunks));
                self.remote_client.put_transaction(&tx_uuid, &parent, &tx_chunks)?;
                
            },
            None => {
                d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &self.remote_head, &tx_chunks));
                self.remote_client.put_transaction(&tx_uuid, self.remote_head, &tx_chunks)?;
            }
        }

        d(&format!("updating rolling head: {:?}", tx_uuid));
        self.rolling_temp_head = Some(tx_uuid.clone());

        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}
