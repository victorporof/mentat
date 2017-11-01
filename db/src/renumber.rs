// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use rusqlite;
use rusqlite::types::{
    ToSql,
};

use mentat_core::{
    SQLValueType,
    ValueType,
};

use errors::{
    Result,
};

use db::{
    read_db,
    update_partition_map,
};

use types::{
    DB,
    PartitionMap,
};

/// Make room in the local store for new entities allocated remotely, shifting local entities to be
/// numbered after remote entities.
///
/// This leaves a "hole" in the entid sequence into which the entities introduced between the `root`
/// and `remote` partitions fit.
///
/// If successful, returns a new `DB` that reflects the new (local) partition map and the new
/// (local) schema after renumbering.
fn renumber(conn: &rusqlite::Connection, root: &PartitionMap, local: &PartitionMap, remote: &PartitionMap) -> Result<DB> {
    // For each partition present in both local and remote, that has advanced locally since remote,
    // renumber local entities.
    // Take each partition's largest value from either local or remote, if only one has advanced.
    let mut output = remote.clone();
    for (name, local_partition) in local {
        if let Some(remote_partition) = remote.get(name) {
            // The partition exists in both places.
            if local_partition.start != remote_partition.start {
                // Oh dear.
                unimplemented!();
            }

            if let Some(root_partition) = root.get(name) {
                let root_index = root_partition.index;
                let advanced_locally_by = local_partition.index - root_index;
                let advanced_remotely_by = remote_partition.index - root_index;
                if advanced_locally_by > 0 && advanced_remotely_by > 0 {
                    // Both changed. Renumber locally.
                    // Note that we bound the set to include the max in order to avoid renumbering other partitions!
                    // TODO: we're going to renumber in the schema, so we will know precisely which attributes
                    // we might need to renumber. Save some work!
                    let lower = root_index;
                    let upper = lower + advanced_locally_by;
                    let increment = advanced_remotely_by;
                    let entity_tag = ValueType::Ref.value_type_tag();

                    let datoms_e = "UPDATE datoms SET e = e + ? WHERE e >= ? AND e < ?";
                    let datoms_a = "UPDATE datoms SET a = a + ? WHERE a >= ? AND a < ?";
                    let datoms_v = "UPDATE datoms SET v = v + ? WHERE value_type_tag = ? AND v >= ? AND v < ?";

                    // TODO: filter by tx, too -- no need to touch historical values, because they can't
                    // possibly be affected.
                    let tx_e = "UPDATE transactions SET e = e + ? WHERE e >= ? AND e < ?";
                    let tx_a = "UPDATE transactions SET a = a + ? WHERE a >= ? AND a < ?";
                    let tx_v = "UPDATE transactions SET v = v + ? WHERE value_type_tag = ? AND v >= ? AND v < ?";
                    let tx_tx = "UPDATE transactions SET tx = tx + ? WHERE tx >= ? AND tx < ?";

                    // Well, this is awkward.
                    let entid_args: Vec<&ToSql> = vec![&increment, &lower, &upper];
                    let value_args: Vec<&ToSql> = vec![&increment, &entity_tag, &lower, &upper];

                    conn.execute(datoms_e, &entid_args)?;
                    conn.execute(datoms_a, &entid_args)?;
                    conn.execute(datoms_v, &value_args)?;

                    conn.execute(tx_e, &entid_args)?;
                    conn.execute(tx_a, &entid_args)?;
                    conn.execute(tx_v, &value_args)?;
                    conn.execute(tx_tx, &entid_args)?;

                    let idents_e = "UPDATE idents SET e = e + ? WHERE e >= ? AND e < ?";
                    let idents_a = "UPDATE idents SET a = a + ? WHERE a >= ? AND a < ?";
                    let idents_v = "UPDATE idents SET v = v + ? WHERE value_type_tag = ? AND v >= ? AND v < ?";

                    conn.execute(idents_e, &entid_args)?;
                    conn.execute(idents_a, &entid_args)?;
                    conn.execute(idents_v, &value_args)?;

                    let schema_e = "UPDATE schema SET e = e + ? WHERE e >= ? AND e < ?";
                    let schema_a = "UPDATE schema SET a = a + ? WHERE a >= ? AND a < ?";
                    let schema_v = "UPDATE schema SET v = v + ? WHERE value_type_tag = ? AND v >= ? AND v < ?";

                    conn.execute(schema_e, &entid_args)?;
                    conn.execute(schema_a, &entid_args)?;
                    conn.execute(schema_v, &value_args)?;

                    output.get_mut(name).unwrap().index += advanced_locally_by;
                } else {
                    // Take the largest value. We already defaulted to remote….
                    if local_partition.index > remote_partition.index {
                        output.get_mut(name).unwrap().index = local_partition.index;
                    }
                }
            } else {
                // There is no partition in the root! Oh dear.
                // This must be implemented before we can add partitions.
                unimplemented!();
            }
        } else {
            // There is no conflict: there is no remote partition with this name.
            output.insert(name.clone(), local_partition.clone());
        }
    }

    // Now update parts materialized view to match.
    update_partition_map(conn, &output)?;

    // This is not the most efficient way to do this, but it'll do for now.
    read_db(conn)
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    extern crate env_logger;

    use super::*;

    use edn;

    use mentat_core::{
        Keyword,
    };

    use db::tests::{
        TestConn,
    };

    // Macro to parse a `Borrow<str>` to an `edn::Value` and assert the given `edn::Value` `matches`
    // against it.
    //
    // This is a macro only to give nice line numbers when tests fail.
    //
    // Duplicated because test-only macro exports are a pain I cannot face.
    macro_rules! assert_matches {
        ( $input: expr, $expected: expr ) => {{
            // Failure to parse the expected pattern is a coding error, so we unwrap.
            let pattern_value = edn::parse::value($expected.borrow())
                .expect(format!("to be able to parse expected {}", $expected).as_str())
                .without_spans();
            assert!($input.matches(&pattern_value),
                    "Expected value:\n{}\nto match pattern:\n{}\n",
                    $input.to_pretty(120).unwrap(),
                    pattern_value.to_pretty(120).unwrap());
        }}
    }

    #[test]
    fn test_renumber_no_change() {
        let mut local = TestConn::default();
        let mut remote = TestConn::default();

        local.transact(r#"[[:db/add 100 :db.schema/version 1]
                           [:db/add 101 :db.schema/version 2]]"#).expect("transact local 1");

        remote.transact(r#"[[:db/add 100 :db.schema/version 1]
                            [:db/add 101 :db.schema/version 2]]"#).expect("transact remote 1");

        assert_eq!(local.partition_map, remote.partition_map);
        assert_eq!(local.schema, remote.schema);

        // N.b., after renumbering, the on-disk data is not reflected in the `TestConn`!
        let mut db = renumber(&local.sqlite,
                              &local.partition_map,
                              &local.partition_map,
                              &remote.partition_map).expect("to renumber");

        // To allow tests to reference "allocated" entids freely, `TestConn` creates a fake
        // partition that doesn't make it to disk.  Add it to what does make it to disk.
        // let fake_partition = Partition { start: 100, index: 1000 };
        db.partition_map.insert(":db.part/fake".into(),
                                local.partition_map.get(":db.part/fake".into()).unwrap().clone());

        // The on-disk partition map or schema should have evolved.
        assert_eq!(local.partition_map, db.partition_map);
        assert_eq!(local.schema, db.schema);
    }

    #[test]
    fn test_renumber_local_fast_forward() {
        let mut local = TestConn::default();
        let mut remote = TestConn::default();

        local.transact(r#"[[:db/add 100 :db.schema/version 1]
                           [:db/add 101 :db.schema/version 2]]"#).expect("transact local 1");

        remote.transact(r#"[[:db/add 100 :db.schema/version 1]
                            [:db/add 101 :db.schema/version 2]]"#).expect("transact remote 1");

        assert_eq!(local.partition_map, remote.partition_map);
        assert_eq!(local.schema, remote.schema);

        local.transact(r#"[[:db/add 103 :db.schema/version 1]
                           [:db/add 104 :db.schema/version 2]]"#).expect("transact remote 2");

        // N.b., after renumbering, the on-disk data is not reflected in the `TestConn`!
        let mut db = renumber(&local.sqlite,
                              &local.partition_map,
                              &local.partition_map,
                              &remote.partition_map).expect("to renumber");

        // To allow tests to reference "allocated" entids freely, `TestConn` creates a fake
        // partition that doesn't make it to disk.  Add it to what does make it to disk.
        // let fake_partition = Partition { start: 100, index: 1000 };
        db.partition_map.insert(":db.part/fake".into(),
                                local.partition_map.get(":db.part/fake".into()).unwrap().clone());

        // Local was ahead of remote, so renumbering is a no-op.
        assert_eq!(local.partition_map, db.partition_map);
        assert_eq!(local.schema, db.schema);
    }

    #[test]
    fn test_renumber_remote_fast_forward() {
        let mut local = TestConn::default();
        let mut remote = TestConn::default();

        local.transact(r#"[[:db/add 100 :db.schema/version 1]
                           [:db/add 101 :db.schema/version 2]]"#).expect("transact local 1");

        remote.transact(r#"[[:db/add 100 :db.schema/version 1]
                            [:db/add 101 :db.schema/version 2]]"#).expect("transact remote 1");

        assert_eq!(local.partition_map, remote.partition_map);
        assert_eq!(local.schema, remote.schema);

        remote.transact(r#"[[:db/add 103 :db.schema/version 1]
                            [:db/add 104 :db.schema/version 2]]"#).expect("transact remote 2");

        // N.b., after renumbering, the on-disk data is not reflected in the `TestConn`!
        let mut db = renumber(&local.sqlite,
                              &local.partition_map,
                              &local.partition_map,
                              &remote.partition_map).expect("to renumber");

        // To allow tests to reference "allocated" entids freely, `TestConn` creates a fake
        // partition that doesn't make it to disk.  Add it to what does make it to disk.
        // let fake_partition = Partition { start: 100, index: 1000 };
        db.partition_map.insert(":db.part/fake".into(),
                                local.partition_map.get(":db.part/fake".into()).unwrap().clone());

        // Now the on-disk partition map should have evolved to match the remote partition map, but
        // the on-disk schema should not have evolved.
        assert_eq!(remote.partition_map, db.partition_map);
        assert_eq!(local.schema, db.schema);
    }

    #[test]
    fn test_renumber_non_trivial_schema() {
        let mut local = TestConn::default();
        let mut remote = TestConn::default();

        local.transact(r#"[[:db/add 100 :db.schema/version 1]
                           [:db/add 101 :db.schema/version 2]]"#).expect("transact local 1");

        remote.transact(r#"[[:db/add 100 :db.schema/version 1]
                            [:db/add 101 :db.schema/version 2]]"#).expect("transact remote 1");

        assert_eq!(local.partition_map, remote.partition_map);
        assert_eq!(local.schema, remote.schema);

        let root = local.partition_map.clone();

        local.transact(r#"[{:db/ident :foo/local :db/valueType :db.type/long}]"#)
            .expect("transact local schema 2");

        remote.transact(r#"[{:db/ident :foo/remote :db/valueType :db.type/string}]"#)
            .expect("transact remote schema 2");

        // last_transaction is deterministic, so this is reproducible but fragile.
        assert_matches!(local.last_transaction(),
                        r#"[[65536 :db/ident :foo/local 268435458 true]
                            [65536 :db/valueType :db.type/long 268435458 true]
                            [268435458 :db/txInstant ?ms 268435458 true]]"#);

        // N.b., after renumbering, the on-disk data is not reflected in the `TestConn`!
        let mut db = renumber(&local.sqlite,
                              &root,
                              &local.partition_map,
                              &remote.partition_map).expect("to renumber");

        // To allow tests to reference "allocated" entids freely, `TestConn` creates a fake
        // partition that doesn't make it to disk.  Add it to what does make it to disk.
        // let fake_partition = Partition { start: 100, index: 1000 };
        db.partition_map.insert(":db.part/fake".into(),
                                local.partition_map.get(":db.part/fake".into()).unwrap().clone());

        // Now the on-disk partition map should have evolved, but it won't match either local or
        // remote.  What it will match is the following "rebased" store's partition map.
        let mut rebased = TestConn::default();
        rebased.transact(r#"[[:db/add 100 :db.schema/version 1]
                             [:db/add 101 :db.schema/version 2]]"#).expect("transact rebased 1");

        rebased.transact(r#"[{:db/ident :foo/remote :db/valueType :db.type/string}]"#)
            .expect("transact rebased schema 2");

        rebased.transact(r#"[{:db/ident :foo/local :db/valueType :db.type/long}]"#)
            .expect("transact rebased schema 3");

        assert_eq!(rebased.partition_map, db.partition_map);

        // However, it won't match the "rebased" store's schema map, since the rebasing hasn't
        // actually take place.  There's a hole in the schema; this fashions the hole by hand.
        let mut schema = rebased.schema.clone();
        let entid = schema.ident_map.remove(&Keyword::namespaced("foo", "remote")).expect("to remove ident");
        assert!(schema.entid_map.remove(&entid).is_some());
        assert!(schema.attribute_map.remove(&entid).is_some());

        assert_eq!(schema, db.schema);

        // Finally, let's verify that the transaction entid has bumped forward in the datoms and the
        // transactions table.  Note that 65537 = 65536 + 1, and 268435459 = 268435458 + 1.
        assert_matches!(local.last_transaction(),
                        r#"[[65537 :db/ident :foo/local 268435459 true]
                            [65537 :db/valueType :db.type/long 268435459 true]
                            [268435459 :db/txInstant ?ms 268435459 true]]"#);
    }
}
