// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;

use errors::{
    DbErrorKind,
    Result,
};

use mentat_core::{
    Entid,
    Schema,
    TypedValue,
    KnownEntid,
};

use edn::{
    InternSet,
};

use edn::entities::OpType;

use db;
use db::{
    TypedSQLValue,
};

use tx::{
    transact_terms_with_action,
    TransactorAction,
};

use types::{
    PartitionMap,
};

use internal_types::{
    Term,
    TermWithoutTempIds,
};

use watcher::{
    NullWatcher,
};

pub static MAIN_TIMELINE: Entid = 0;

fn ensure_on_tail_of(conn: &rusqlite::Connection, sorted_tx_ids: &[Entid], timeline: Entid) -> Result<()> {
    let mut stmt = conn.prepare("SELECT tx, timeline FROM timelined_transactions WHERE tx >= ? AND timeline = ?")?;
    let mut rows = stmt.query_and_then(&[&sorted_tx_ids[0], &timeline], |row: &rusqlite::Row| -> Result<(Entid, Entid)>{
        Ok((row.get_checked(0)?, row.get_checked(1)?))
    })?;

    // Ensure that tx_ids are a consistent block at the tail end of the transactions table.
    // TODO do this in SQL? e.g. SELECT tx FROM timelined_transactions WHERE tx >= ? AND tx NOT IN [tx_ids]...
    let timeline = match rows.next() {
        Some(t) => {
            let t = t?;
            if !sorted_tx_ids.contains(&t.0) {
                println!("1) got tx: {}, list of txs: {:?}", t.0, sorted_tx_ids);
                bail!(DbErrorKind::TimelinesNotOnTail);
            }
            t.1
        },
        None => bail!(DbErrorKind::TimelinesInvalidTransactionIds)
    };

    while let Some(t) = rows.next() {
        let t = t?;
        if !sorted_tx_ids.contains(&t.0) {
            println!("2) got tx: {}, list of txs: {:?}", t.0, sorted_tx_ids);
            bail!(DbErrorKind::TimelinesNotOnTail);
        }
        if t.1 != timeline {
            bail!(DbErrorKind::TimelinesMixed);
        }
    }

    Ok(())
}

fn move_transactions_to(conn: &rusqlite::Connection, tx_ids: &[Entid], new_timeline: Entid) -> Result<()> {
    // Move specified transactions over to a specified timeline.
    conn.execute(&format!(
        "UPDATE timelined_transactions SET timeline = {} WHERE tx IN ({})",
            new_timeline,
            ::repeat_values(tx_ids.len(), 1)
        ), &(tx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())
}

/// Get terms for tx_id, reversing them in meaning (swap add & retract).
fn reversed_terms_for(conn: &rusqlite::Connection, tx_id: Entid) -> Result<Vec<TermWithoutTempIds>> {
    let mut stmt = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM timelined_transactions WHERE tx = ? AND timeline = ? ORDER BY tx DESC")?;
    let mut rows = stmt.query_and_then(&[&tx_id, &MAIN_TIMELINE], |row| -> Result<TermWithoutTempIds> {
        let op = match row.get_checked(5)? {
            true => OpType::Retract,
            false => OpType::Add
        };
        Ok(Term::AddOrRetract(
            op,
            KnownEntid(row.get_checked(0)?),
            row.get_checked(1)?,
            TypedValue::from_sql_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(row) = rows.next() {
        terms.push(row?);
    }
    Ok(terms)
}

/// Move specified tail-end transactions off of main timeline.
/// 
/// It's not possible to determine a high water-mark of partitions
/// when rewinding transactions just by inspection of their datoms.
/// We depend on our caller to supply the correct "target" partition map.
pub fn move_from_main_timeline(conn: &rusqlite::Connection, schema: &Schema,
    partition_map: PartitionMap, tx_ids: &[Entid], new_timeline: Entid) -> Result<(Option<Schema>, PartitionMap)> {

    if new_timeline == MAIN_TIMELINE {
        bail!(DbErrorKind::NotYetImplemented(format!("Can't move transactions to main timeline")));
    }

    let mut sorted_tx_ids = tx_ids.to_vec();
    sorted_tx_ids.sort_by(|t1, t2| t1.cmp(t2));

    ensure_on_tail_of(conn, &sorted_tx_ids, MAIN_TIMELINE)?;

    let mut last_schema = None;
    for tx_id in sorted_tx_ids {
        let reversed_terms = reversed_terms_for(conn, tx_id)?;

        // Rewind schema and datoms.
        let (_, _, new_schema, _) = transact_terms_with_action(
            conn, partition_map.clone(), schema, schema, NullWatcher(),
            reversed_terms.into_iter().map(|t| t.rewrap()),
            InternSet::new(), TransactorAction::Materialize
        )?;
        last_schema = new_schema;
    }

    // Move transactions over to the target timeline.
    move_transactions_to(conn, tx_ids, new_timeline)?;

    Ok((last_schema, db::read_partition_map(conn)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use edn;

    use std::borrow::{
        Borrow,
    };

    use debug::{
        TestConn,
    };

    use bootstrap;

    // TODO I don't think "moving timelines" belongs in a conn (which would handle this),
    // but perhaps it does?
    fn update_conn(conn: &mut TestConn, schema: &Option<Schema>, pmap: &PartitionMap) {
        match schema {
            Some(s) => conn.schema = s.clone(),
            None => ()
        };
        conn.partition_map = pmap.clone();
    }
    
    #[test]
    fn test_pop_simple() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:db/id :db/doc :db/doc "test"}]
        "#;

        let partition_map0 = conn.partition_map.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            &vec![conn.last_tx_id()], 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(new_partition_map, partition_map0);

        conn.partition_map = partition_map0.clone();
        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);

        assert_matches!(conn.datoms(), r#"
            [[37 :db/doc "test"]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[37 :db/doc "test" ?tx true]
              [?tx :db/txInstant ?ms ?tx true]]]
        "#);
    }

    #[test]
    fn test_pop_ident() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:db/ident :test/entid :db/doc "test" :db.schema/version 1}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            &vec![conn.last_tx_id()], 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(conn.partition_map, partition_map1);
        assert_eq!(conn.schema, schema1);

        assert_matches!(conn.datoms(), r#"
            [[?e :db/ident :test/entid]
             [?e :db/doc "test"]
             [?e :db.schema/version 1]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e :db/ident :test/entid ?tx true]
              [?e :db/doc "test" ?tx true]
              [?e :db.schema/version 1 ?tx true]
              [?tx :db/txInstant ?ms ?tx true]]]
        "#);
    }

    
    #[test]
    fn test_pop_schema() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:db/id "e" :db/ident :test/one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
             {:db/id "f" :db/ident :test/many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            &vec![report1.tx_id], 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :db/ident :test/one]
             [?e1 :db/valueType :db.type/long]
             [?e1 :db/cardinality :db.cardinality/one]
             [?e2 :db/ident :test/many]
             [?e2 :db/valueType :db.type/long]
             [?e2 :db/cardinality :db.cardinality/many]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :db/ident :test/one ?tx1 true]
             [?e1 :db/valueType :db.type/long ?tx1 true]
             [?e1 :db/cardinality :db.cardinality/one ?tx1 true]
             [?e2 :db/ident :test/many ?tx1 true]
             [?e2 :db/valueType :db.type/long ?tx1 true]
             [?e2 :db/cardinality :db.cardinality/many ?tx1 true]
             [?tx1 :db/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_in_sequence() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65538),
                   conn.partition_map.allocate_entids(":db.part/user", 2));
        let tx_report0 = assert_transact!(conn, r#"[
            {:db/id 65536 :db/ident :test/one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
            {:db/id 65537 :db/ident :test/many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
        ]"#);

        let first = "[
            [65536 :db/ident :test/one]
            [65536 :db/valueType :db.type/long]
            [65536 :db/cardinality :db.cardinality/one]
            [65537 :db/ident :test/many]
            [65537 :db/valueType :db.type/long]
            [65537 :db/cardinality :db.cardinality/many]
        ]";
        assert_matches!(conn.datoms(), first);

        let partition_map0 = conn.partition_map.clone();

        assert_eq!((65538..65539),
                   conn.partition_map.allocate_entids(":db.part/user", 1));
        let tx_report1 = assert_transact!(conn, r#"[
            [:db/add 65538 :test/one 1]
            [:db/add 65538 :test/many 2]
            [:db/add 65538 :test/many 3]
        ]"#);
        let schema1 = conn.schema.clone();
        let partition_map1 = conn.partition_map.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx true]
                          [65538 :test/many 2 ?tx true]
                          [65538 :test/many 3 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        let second = "[
            [65536 :db/ident :test/one]
            [65536 :db/valueType :db.type/long]
            [65536 :db/cardinality :db.cardinality/one]
            [65537 :db/ident :test/many]
            [65537 :db/valueType :db.type/long]
            [65537 :db/cardinality :db.cardinality/many]
            [65538 :test/one 1]
            [65538 :test/many 2]
            [65538 :test/many 3]
        ]";
        assert_matches!(conn.datoms(), second);

        let tx_report2 = assert_transact!(conn, r#"[
            [:db/add 65538 :test/one 2]
            [:db/add 65538 :test/many 2]
            [:db/retract 65538 :test/many 3]
            [:db/add 65538 :test/many 4]
        ]"#);
        let schema2 = conn.schema.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx false]
                          [65538 :test/one 2 ?tx true]
                          [65538 :test/many 3 ?tx false]
                          [65538 :test/many 4 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        let third = "[
            [65536 :db/ident :test/one]
            [65536 :db/valueType :db.type/long]
            [65536 :db/cardinality :db.cardinality/one]
            [65537 :db/ident :test/many]
            [65537 :db/valueType :db.type/long]
            [65537 :db/cardinality :db.cardinality/many]
            [65538 :test/one 2]
            [65538 :test/many 2]
            [65538 :test/many 4]
        ]";
        assert_matches!(conn.datoms(), third);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            &vec![tx_report2.tx_id], 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), second);
        // Moving didn't change the schema.
        assert_eq!(None, new_schema);
        assert_eq!(conn.schema, schema2);
        // But it did change the partition map.
        assert_eq!(conn.partition_map, partition_map1);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            &vec![tx_report1.tx_id], 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_matches!(conn.datoms(), first);
        assert_eq!(None, new_schema);
        assert_eq!(schema1, conn.schema);
        assert_eq!(conn.partition_map, partition_map0);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            &vec![tx_report0.tx_id], 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_eq!(true, new_schema.is_some());
        assert_eq!(bootstrap::bootstrap_schema(), conn.schema);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }
}
