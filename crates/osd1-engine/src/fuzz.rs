//! Seeded differential fuzz against a second bundled SQLite connection.
//!
//! Each step is one OSD1 request: `BEGIN IMMEDIATE`, the SQL, then `COMMIT`
//! or `ROLLBACK`. The reference connection does the same. Client `BEGIN` is
//! generated only as a statement that must be rejected.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use osd1_types::{ResultShape, SqlRequest, SqlValue};
use rusqlite::{params_from_iter, types::ValueRef, Batch, Connection};

use super::SqliteWorker;

const WORDS: &[&str] = &["a", "b", "ada", "sql", "row", "n", ""];
const REALS: [f64; 4] = [0.5, 1.5, -1.0, 2.0];

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn int(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }

    fn bool(&mut self, n: usize) -> bool {
        self.int(n) == 0
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.int(items.len())]
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ColKind {
    Int,
    Text,
    Real,
}

impl ColKind {
    fn sql(self) -> &'static str {
        match self {
            ColKind::Int => "INTEGER",
            ColKind::Text => "TEXT",
            ColKind::Real => "REAL",
        }
    }
}

#[derive(Clone)]
struct RefTarget {
    table: String,
    column: String,
}

#[derive(Clone)]
struct Col {
    name: String,
    kind: ColKind,
    pk: bool,
    notnull: bool,
    unique: bool,
    references: Option<RefTarget>,
    cascade: bool,
}

#[derive(Clone)]
struct Table {
    name: String,
    cols: Vec<Col>,
    next_col: usize,
    pinned: bool,
}

impl Table {
    fn pk(&self) -> &Col {
        self.cols.iter().find(|col| col.pk).expect("primary key")
    }
}

#[derive(Clone)]
struct Index {
    name: String,
    table: String,
    column: String,
    plain: bool,
}

#[derive(Clone)]
struct View {
    name: String,
    table: String,
}

#[derive(Clone)]
struct Trigger {
    name: String,
    table: String,
}

enum Effect {
    None,
    AddTable(Table),
    AddIndex(Index),
    DropIndex(String),
    DropTable(String),
    AddColumn { table: String, col: Col },
    AddView(View),
    DropView(String),
    AddTrigger(Trigger),
    DropTrigger(String),
}

struct Generated {
    sql: String,
    params: Vec<SqlValue>,
    script: bool,
    denied: bool,
    expect_err: bool,
    effect: Effect,
}

struct Model {
    tables: Vec<Table>,
    indexes: Vec<Index>,
    views: Vec<View>,
    triggers: Vec<Trigger>,
    next_table: u64,
    next_index: u64,
    next_view: u64,
    next_trigger: u64,
}

impl Model {
    fn new() -> Self {
        Self {
            tables: Vec::new(),
            indexes: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            next_table: 0,
            next_index: 0,
            next_view: 0,
            next_trigger: 0,
        }
    }

    fn generate(&mut self, rng: &mut Rng) -> Generated {
        for _ in 0..40 {
            let kind = rng.int(120);
            let generated = match kind {
                0..=7 => self.create_table(rng),
                8..=13 => self.create_index(rng),
                14..=16 => self.drop_index(rng),
                17..=30 => self.insert(rng, false),
                31..=36 => self.insert(rng, true),
                37..=46 => self.script(rng, false),
                47..=52 => self.script(rng, true),
                53..=62 => self.update(rng),
                63..=72 => self.delete(rng),
                73..=80 => self.search_point(rng),
                81..=85 => self.search_scan(rng),
                86..=89 => self.search_join(rng),
                90..=93 => self.search_agg(rng),
                94..=97 => self.search_expr(rng),
                98..=100 => self.view(rng),
                101..=103 => self.upsert(rng),
                104..=105 => self.replace_row(rng),
                106..=107 => self.trigger(rng),
                108..=109 => self.explain(rng),
                110 => self.reindex(rng),
                111 => self.analyze(),
                112 => self.vacuum(),
                113..=114 => self.alter(rng),
                115 => self.check_pragma(rng),
                _ => self.denied(rng),
            };
            if let Some(generated) = generated {
                return generated;
            }
        }
        self.create_table(rng).expect("create table")
    }

    fn apply(&mut self, effect: Effect) {
        match effect {
            Effect::None => {}
            Effect::AddTable(table) => self.tables.push(table),
            Effect::AddIndex(index) => self.indexes.push(index),
            Effect::DropIndex(name) => self.indexes.retain(|index| index.name != name),
            Effect::DropTable(name) => {
                self.tables.retain(|table| table.name != name);
                self.indexes.retain(|index| index.table != name);
                self.triggers.retain(|trigger| trigger.table != name);
                self.views.retain(|view| view.table != name);
            }
            Effect::AddColumn { table, col } => {
                if let Some(found) = self.tables.iter_mut().find(|item| item.name == table) {
                    found.next_col += 1;
                    found.cols.push(col);
                }
            }
            Effect::AddView(view) => self.views.push(view),
            Effect::DropView(name) => self.views.retain(|view| view.name != name),
            Effect::AddTrigger(trigger) => self.triggers.push(trigger),
            Effect::DropTrigger(name) => self.triggers.retain(|trigger| trigger.name != name),
        }
    }

    fn user_tables(&self) -> Vec<&Table> {
        self.tables.iter().filter(|table| !table.pinned).collect()
    }

    fn any_table<'a>(&'a self, rng: &mut Rng) -> Option<&'a Table> {
        if self.tables.is_empty() {
            None
        } else {
            Some(rng.pick(&self.tables))
        }
    }

    fn create_table(&mut self, rng: &mut Rng) -> Option<Generated> {
        if self.user_tables().len() >= 8 {
            return None;
        }
        let name = format!("t{}", self.next_table);
        self.next_table += 1;
        let mut cols = vec![Col {
            name: "c0".to_string(),
            kind: ColKind::Int,
            pk: true,
            notnull: false,
            unique: false,
            references: None,
            cascade: false,
        }];
        for index in 0..rng.int(4) {
            let kind = match rng.int(3) {
                0 => ColKind::Int,
                1 => ColKind::Text,
                _ => ColKind::Real,
            };
            let references = if kind == ColKind::Int && !self.tables.is_empty() && rng.bool(2) {
                let parent = rng.pick(&self.tables);
                Some(RefTarget {
                    table: parent.name.clone(),
                    column: parent.pk().name.clone(),
                })
            } else {
                None
            };
            let cascade = references.is_some() && rng.bool(2);
            cols.push(Col {
                name: format!("c{}", index + 1),
                kind,
                pk: false,
                notnull: rng.bool(4),
                unique: references.is_none() && rng.bool(5),
                references,
                cascade,
            });
        }
        let sql = render_create(&name, &cols);
        let table = Table {
            name,
            next_col: cols.len(),
            pinned: false,
            cols,
        };
        Some(ok(sql, Vec::new(), false, Effect::AddTable(table)))
    }

    fn create_index(&mut self, rng: &mut Rng) -> Option<Generated> {
        if self.indexes.len() >= 12 {
            return None;
        }
        let (table_name, col_name, kind) = {
            let table = self.any_table(rng)?;
            let col = rng.pick(&table.cols);
            (table.name.clone(), col.name.clone(), col.kind)
        };
        let name = format!("idx{}", self.next_index);
        self.next_index += 1;
        let expression = kind == ColKind::Text && rng.bool(4);
        let partial = !expression && rng.bool(4);
        let unique = !expression && !partial && rng.bool(3);
        let target = if expression {
            format!("lower({col_name})")
        } else {
            col_name.clone()
        };
        let mut sql = format!("CREATE ");
        if unique {
            sql.push_str("UNIQUE ");
        }
        sql.push_str(&format!("INDEX {name} ON {table_name} ({target})"));
        if partial {
            sql.push_str(&format!(" WHERE {col_name} > 0"));
        }
        let index = Index {
            name,
            table: table_name,
            column: col_name,
            plain: !expression && !partial,
        };
        Some(ok(sql, Vec::new(), false, Effect::AddIndex(index)))
    }

    fn drop_index(&mut self, rng: &mut Rng) -> Option<Generated> {
        if self.indexes.is_empty() {
            return None;
        }
        let index = rng.pick(&self.indexes).name.clone();
        Some(ok(
            format!("DROP INDEX {index}"),
            Vec::new(),
            false,
            Effect::DropIndex(index),
        ))
    }

    fn insert(&mut self, rng: &mut Rng, many: bool) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let rows = if many { 2 + rng.int(2) } else { 1 };
        let use_params = !many && rng.bool(2);
        let or_clause = match rng.int(6) {
            0 => " OR IGNORE",
            1 => " OR REPLACE",
            _ => "",
        };
        let mut names = Vec::new();
        for col in &table.cols {
            let keep = col.pk || col.notnull || col.unique || rng.bool(2);
            if col.pk && rng.bool(5) {
                continue;
            }
            if keep {
                names.push(col.name.clone());
            }
        }
        if names.is_empty() {
            return Some(ok(
                format!("INSERT{or_clause} INTO {} DEFAULT VALUES", table.name),
                Vec::new(),
                false,
                Effect::None,
            ));
        }
        let mut params = Vec::new();
        let mut tuples = Vec::new();
        for _ in 0..rows {
            let mut lits = Vec::new();
            for name in &names {
                let col = table.cols.iter().find(|col| col.name == *name).unwrap();
                let value = value_for(rng, col);
                if use_params {
                    params.push(value);
                    lits.push("?".to_string());
                } else {
                    lits.push(sql_lit(&value));
                }
            }
            tuples.push(format!("({})", lits.join(", ")));
        }
        let sql = format!(
            "INSERT{or_clause} INTO {} ({}) VALUES {}",
            table.name,
            names.join(", "),
            tuples.join(", ")
        );
        Some(ok(sql, params, false, Effect::None))
    }

    fn script(&mut self, rng: &mut Rng, rollback: bool) -> Option<Generated> {
        let table = self
            .tables
            .iter()
            .find(|table| easy_insert(table) && table.pk().kind == ColKind::Int)?
            .clone();
        let key = table.pk().name.clone();
        let key_value = if rollback {
            10_000 + rng.int(1_000_000) as i64
        } else {
            rng.int(16) as i64 - 2
        };
        let assignments = filled_literals(&table, &key, key_value);
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table.name, assignments.0, assignments.1
        );
        if rollback {
            let sql = format!("{insert}; {insert}");
            return Some(Generated {
                sql,
                params: Vec::new(),
                script: true,
                denied: false,
                expect_err: true,
                effect: Effect::None,
            });
        }
        let touch = table
            .cols
            .iter()
            .find(|col| !col.pk)
            .map(|col| match col.kind {
                ColKind::Text => format!(
                    "UPDATE {} SET {} = upper({}) WHERE {} = {key_value}",
                    table.name, col.name, col.name, key
                ),
                ColKind::Int => format!(
                    "UPDATE {} SET {} = COALESCE({}, 0) + 1 WHERE {} = {key_value}",
                    table.name, col.name, col.name, key
                ),
                ColKind::Real => format!(
                    "UPDATE {} SET {} = COALESCE({}, 0) + 0.5 WHERE {} = {key_value}",
                    table.name, col.name, col.name, key
                ),
            })
            .unwrap_or_else(|| {
                format!(
                    "UPDATE {} SET {key} = {key} WHERE {key} = {key_value}",
                    table.name
                )
            });
        let select = format!(
            "SELECT rowid, * FROM {} WHERE {key} = {key_value} ORDER BY rowid",
            table.name
        );
        Some(Generated {
            sql: format!("{insert}; {touch}; {select}"),
            params: Vec::new(),
            script: true,
            denied: false,
            expect_err: false,
            effect: Effect::None,
        })
    }

    fn update(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let col = rng.pick(&table.cols).clone();
        let pk = table.pk().clone();
        let assignment = match col.kind {
            ColKind::Text if !col.pk => format!("{} = upper({})", col.name, col.name),
            ColKind::Int if !col.pk => format!("{} = COALESCE({}, 0) + 1", col.name, col.name),
            ColKind::Real if !col.pk => format!("{} = COALESCE({}, 0) + 0.5", col.name, col.name),
            _ => {
                let value = value_for(rng, &col);
                if rng.bool(2) {
                    return Some(ok(
                        format!(
                            "UPDATE {} SET {} = ? WHERE {} = ?",
                            table.name, col.name, pk.name
                        ),
                        vec![value, value_for(rng, &pk)],
                        false,
                        Effect::None,
                    ));
                }
                format!("{} = {}", col.name, sql_lit(&value))
            }
        };
        let sql = if rng.bool(8) {
            format!("UPDATE {} SET {assignment}", table.name)
        } else {
            let probe = value_for(rng, &pk);
            if rng.bool(2) {
                return Some(ok(
                    format!(
                        "UPDATE {} SET {assignment} WHERE {} = ?",
                        table.name, pk.name
                    ),
                    vec![probe],
                    false,
                    Effect::None,
                ));
            }
            format!(
                "UPDATE {} SET {assignment} WHERE {} = {}",
                table.name,
                pk.name,
                sql_lit(&probe)
            )
        };
        Some(ok(sql, Vec::new(), false, Effect::None))
    }

    fn delete(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?;
        let sql = if rng.bool(8) {
            format!("DELETE FROM {}", table.name)
        } else if rng.bool(3) {
            format!(
                "DELETE FROM {} WHERE rowid IN (SELECT rowid FROM {} ORDER BY rowid LIMIT 1)",
                table.name, table.name
            )
        } else {
            let pk = table.pk();
            let probe = value_for(rng, pk);
            if rng.bool(2) {
                return Some(ok(
                    format!("DELETE FROM {} WHERE {} = ?", table.name, pk.name),
                    vec![probe],
                    false,
                    Effect::None,
                ));
            }
            format!(
                "DELETE FROM {} WHERE {} = {}",
                table.name,
                pk.name,
                sql_lit(&probe)
            )
        };
        Some(ok(sql, Vec::new(), false, Effect::None))
    }

    fn search_point(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?;
        let indexed = self
            .indexes
            .iter()
            .find(|index| index.table == table.name && index.plain);
        let (column, indexed_by) = if let Some(index) = indexed {
            if rng.bool(2) {
                (index.column.clone(), format!(" INDEXED BY {}", index.name))
            } else {
                (table.pk().name.clone(), String::new())
            }
        } else {
            (table.pk().name.clone(), String::new())
        };
        let col = table.cols.iter().find(|col| col.name == column).unwrap();
        let probe = value_for(rng, col);
        if rng.bool(2) {
            return Some(ok(
                format!(
                    "SELECT rowid, * FROM {}{indexed_by} WHERE {column} = ? ORDER BY rowid",
                    table.name
                ),
                vec![probe],
                false,
                Effect::None,
            ));
        }
        Some(ok(
            format!(
                "SELECT rowid, * FROM {}{indexed_by} WHERE {column} = {} ORDER BY rowid",
                table.name,
                sql_lit(&probe)
            ),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn search_scan(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let pk = table.pk().name.clone();
        if let Some(text) = table.cols.iter().find(|col| col.kind == ColKind::Text) {
            if rng.bool(2) {
                let word = *rng.pick(WORDS);
                return Some(ok(
                    format!(
                        "SELECT rowid, * FROM {} WHERE {} LIKE ? ORDER BY rowid",
                        table.name, text.name
                    ),
                    vec![SqlValue::Text(format!("%{word}%"))],
                    false,
                    Effect::None,
                ));
            }
        }
        let low = rng.int(8) as i64 - 2;
        let high = low + 1 + rng.int(6) as i64;
        Some(ok(
            format!(
                "SELECT rowid, * FROM {} WHERE {pk} > {low} AND {pk} < {high} ORDER BY rowid LIMIT 8 OFFSET {}",
                table.name,
                rng.int(3)
            ),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn search_join(&mut self, rng: &mut Rng) -> Option<Generated> {
        if self.tables.len() < 2 {
            return None;
        }
        let left = self.tables[rng.int(self.tables.len())].clone();
        let right = self.tables[rng.int(self.tables.len())].clone();
        if left.name == right.name {
            return None;
        }
        Some(ok(
            format!(
                "SELECT a.rowid, b.rowid, a.{lpk}, b.{rpk} FROM {} AS a JOIN {} AS b ON a.{lpk} = b.{rpk} ORDER BY a.rowid, b.rowid",
                left.name,
                right.name,
                lpk = left.pk().name,
                rpk = right.pk().name
            ),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn search_agg(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let col = rng.pick(&table.cols).clone();
        Some(ok(
            format!(
                "SELECT {name}, COUNT(*) AS cnt FROM {} GROUP BY {name} HAVING cnt >= 0 ORDER BY {name}, cnt",
                table.name,
                name = col.name
            ),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn search_expr(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let sql = match rng.int(4) {
            0 => format!(
                "SELECT rowid, typeof({pk}), length(COALESCE({text}, '')), abs(COALESCE({pk}, 0)) FROM {} ORDER BY rowid",
                table.name,
                pk = table.pk().name,
                text = table
                    .cols
                    .iter()
                    .find(|col| col.kind == ColKind::Text)
                    .map(|col| col.name.as_str())
                    .unwrap_or(table.pk().name.as_str())
            ),
            1 => format!(
                "SELECT rowid, {pk}, ROW_NUMBER() OVER (ORDER BY rowid) AS r FROM {} ORDER BY rowid",
                table.name,
                pk = table.pk().name
            ),
            2 => format!(
                "WITH s AS (SELECT rowid AS rid, {pk} AS k FROM {} WHERE {pk} IS NOT NULL) SELECT rid, k FROM s ORDER BY rid",
                table.name,
                pk = table.pk().name
            ),
            _ => {
                if self.tables.len() < 2 {
                    format!(
                        "SELECT DISTINCT {pk} AS k FROM {} ORDER BY k",
                        table.name,
                        pk = table.pk().name
                    )
                } else {
                    let other = self.tables.iter().find(|item| item.name != table.name).unwrap();
                    format!(
                        "SELECT {pk} AS k FROM {} WHERE {pk} IN (SELECT {} FROM {}) UNION SELECT {pk} FROM {} ORDER BY k",
                        table.name,
                        other.pk().name,
                        other.name,
                        table.name,
                        pk = table.pk().name
                    )
                }
            }
        };
        Some(ok(sql, Vec::new(), false, Effect::None))
    }

    fn view(&mut self, rng: &mut Rng) -> Option<Generated> {
        if !self.views.is_empty() && rng.bool(3) {
            let name = rng.pick(&self.views).name.clone();
            return Some(ok(
                format!("DROP VIEW {name}"),
                Vec::new(),
                false,
                Effect::DropView(name),
            ));
        }
        if !self.views.is_empty() && rng.bool(2) {
            let view = rng.pick(&self.views).name.clone();
            return Some(ok(
                format!("SELECT rid, k FROM {view} ORDER BY rid"),
                Vec::new(),
                false,
                Effect::None,
            ));
        }
        if self.views.len() >= 4 {
            return None;
        }
        let (table_name, pk) = {
            let choices = self.user_tables();
            if choices.is_empty() {
                return None;
            }
            let table = *rng.pick(&choices);
            (table.name.clone(), table.pk().name.clone())
        };
        let name = format!("v{}", self.next_view);
        self.next_view += 1;
        let sql = format!(
            "CREATE VIEW {name} AS SELECT rowid AS rid, {pk} AS k FROM {table_name} WHERE {pk} IS NOT NULL"
        );
        Some(ok(
            sql,
            Vec::new(),
            false,
            Effect::AddView(View {
                name,
                table: table_name,
            }),
        ))
    }

    fn upsert(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let pk = table.pk();
        let key = value_for(rng, pk);
        let other = table.cols.iter().find(|col| !col.pk);
        let (cols, values, update) = if let Some(col) = other {
            let value = value_for(rng, col);
            (
                format!("{}, {}", pk.name, col.name),
                format!("{}, {}", sql_lit(&key), sql_lit(&value)),
                format!("DO UPDATE SET {} = excluded.{}", col.name, col.name),
            )
        } else {
            (pk.name.clone(), sql_lit(&key), "DO NOTHING".to_string())
        };
        Some(ok(
            format!(
                "INSERT INTO {} ({cols}) VALUES ({values}) ON CONFLICT ({}) {update}",
                table.name, pk.name
            ),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn replace_row(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?.clone();
        let (cols, vals) = filled_literals(&table, &table.pk().name, rng.int(16) as i64 - 2);
        Some(ok(
            format!("REPLACE INTO {} ({cols}) VALUES ({vals})", table.name),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn trigger(&mut self, rng: &mut Rng) -> Option<Generated> {
        if self.triggers.len() >= 4 {
            return None;
        }
        if !self.triggers.is_empty() && rng.bool(3) {
            let name = rng.pick(&self.triggers).name.clone();
            return Some(ok(
                format!("DROP TRIGGER {name}"),
                Vec::new(),
                false,
                Effect::DropTrigger(name),
            ));
        }
        let (table_name, pk) = {
            let choices = self.user_tables();
            if choices.is_empty() {
                return None;
            }
            let table = *rng.pick(&choices);
            (table.name.clone(), table.pk().name.clone())
        };
        let name = format!("tr{}", self.next_trigger);
        self.next_trigger += 1;
        let (when, old_new) = if rng.bool(2) {
            ("INSERT", "NEW")
        } else {
            ("DELETE", "OLD")
        };
        let sql = format!(
            "CREATE TRIGGER {name} AFTER {when} ON {table_name} BEGIN INSERT INTO audit (src, n) VALUES ('{name}', {old_new}.{pk}); END"
        );
        Some(ok(
            sql,
            Vec::new(),
            true,
            Effect::AddTrigger(Trigger {
                name,
                table: table_name,
            }),
        ))
    }

    fn explain(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?;
        let pk = &table.pk().name;
        Some(ok(
            format!(
                "EXPLAIN QUERY PLAN SELECT * FROM {} WHERE {pk} = 1",
                table.name
            ),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn reindex(&mut self, rng: &mut Rng) -> Option<Generated> {
        let table = self.any_table(rng)?;
        Some(ok(
            format!("REINDEX {}", table.name),
            Vec::new(),
            false,
            Effect::None,
        ))
    }

    fn analyze(&mut self) -> Option<Generated> {
        if self.tables.is_empty() {
            None
        } else {
            Some(ok("ANALYZE".to_string(), Vec::new(), false, Effect::None))
        }
    }

    fn vacuum(&mut self) -> Option<Generated> {
        Some(Generated {
            sql: "VACUUM".to_string(),
            params: Vec::new(),
            script: false,
            denied: false,
            expect_err: true,
            effect: Effect::None,
        })
    }

    fn alter(&mut self, rng: &mut Rng) -> Option<Generated> {
        let choices = self.user_tables();
        if choices.is_empty() {
            return None;
        }
        let table = *rng.pick(&choices);
        if table.cols.len() >= 8 {
            return None;
        }
        let name = format!("c{}", table.next_col);
        let kind = match rng.int(3) {
            0 => ColKind::Int,
            1 => ColKind::Text,
            _ => ColKind::Real,
        };
        let col = Col {
            name: name.clone(),
            kind,
            pk: false,
            notnull: false,
            unique: false,
            references: None,
            cascade: false,
        };
        Some(ok(
            format!(
                "ALTER TABLE {} ADD COLUMN {name} {}",
                table.name,
                kind.sql()
            ),
            Vec::new(),
            false,
            Effect::AddColumn {
                table: table.name.clone(),
                col,
            },
        ))
    }

    fn check_pragma(&mut self, rng: &mut Rng) -> Option<Generated> {
        let sql = if rng.bool(2) {
            "PRAGMA integrity_check".to_string()
        } else {
            "SELECT \"table\", rowid, parent, fkid FROM pragma_foreign_key_check ORDER BY 1, 2, 3, 4"
                .to_string()
        };
        Some(ok(sql, Vec::new(), false, Effect::None))
    }

    fn denied(&mut self, rng: &mut Rng) -> Option<Generated> {
        let choices = [
            "BEGIN",
            "BEGIN IMMEDIATE",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT fuzz",
            "RELEASE fuzz",
            "ATTACH DATABASE ':memory:' AS other",
            "DETACH DATABASE main",
            "PRAGMA writable_schema=ON",
            "SELECT load_extension('nope')",
            "SELECT readfile('/etc/hosts')",
        ];
        let sql = *rng.pick(&choices);
        Some(Generated {
            sql: sql.to_string(),
            params: Vec::new(),
            script: false,
            denied: true,
            expect_err: true,
            effect: Effect::None,
        })
    }

    fn drop_table_op(&mut self, rng: &mut Rng) -> Option<Generated> {
        let choices = self.user_tables();
        if choices.is_empty() {
            return None;
        }
        let name = rng.pick(&choices).name.clone();
        Some(ok(
            format!("DROP TABLE {name}"),
            Vec::new(),
            false,
            Effect::DropTable(name),
        ))
    }
}

fn ok(sql: String, params: Vec<SqlValue>, script: bool, effect: Effect) -> Generated {
    Generated {
        sql,
        params,
        script,
        denied: false,
        expect_err: false,
        effect,
    }
}

fn easy_insert(table: &Table) -> bool {
    table
        .cols
        .iter()
        .all(|col| !col.notnull || col.pk || col.references.is_none())
}

fn filled_literals(table: &Table, key: &str, key_value: i64) -> (String, String) {
    let mut names = Vec::new();
    let mut vals = Vec::new();
    for col in &table.cols {
        if col.references.is_some() && !col.notnull {
            continue;
        }
        if !col.pk && !col.notnull && !col.unique {
            continue;
        }
        names.push(col.name.as_str());
        if col.name == key {
            vals.push(key_value.to_string());
        } else {
            vals.push(match col.kind {
                ColKind::Int => "0".to_string(),
                ColKind::Text => "'x'".to_string(),
                ColKind::Real => "0.5".to_string(),
            });
        }
    }
    if names.is_empty() {
        names.push(key);
        vals.push(key_value.to_string());
    }
    (names.join(", "), vals.join(", "))
}

fn value_for(rng: &mut Rng, col: &Col) -> SqlValue {
    if !col.pk && !col.notnull && rng.bool(5) {
        return SqlValue::Null;
    }
    match col.kind {
        ColKind::Int => SqlValue::Integer(rng.int(16) as i64 - 2),
        ColKind::Text => SqlValue::Text((*rng.pick(WORDS)).to_string()),
        ColKind::Real => SqlValue::Real(REALS[rng.int(REALS.len())]),
    }
}

fn sql_lit(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::Real(value) => {
            let text = format!("{value}");
            if text.contains('.') || text.contains('e') || text.contains('E') {
                text
            } else {
                format!("{text}.0")
            }
        }
        SqlValue::Text(value) => format!("'{}'", value.replace('\'', "''")),
    }
}

fn render_create(name: &str, cols: &[Col]) -> String {
    let body = cols
        .iter()
        .map(|col| {
            let mut sql = format!("{} {}", col.name, col.kind.sql());
            if col.pk {
                sql.push_str(" PRIMARY KEY");
            }
            if col.notnull && !col.pk {
                sql.push_str(" NOT NULL");
            }
            if col.unique {
                sql.push_str(" UNIQUE");
            }
            if let Some(target) = &col.references {
                sql.push_str(&format!(" REFERENCES {}({})", target.table, target.column));
                if col.cascade {
                    sql.push_str(" ON DELETE CASCADE");
                }
            }
            sql
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE {name} ({body})")
}

#[derive(Clone, Debug, PartialEq)]
struct Canon {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    changes: u64,
    last_row_id: i64,
}

fn canon_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "null".to_string(),
        SqlValue::Integer(value) => format!("i:{value}"),
        SqlValue::Real(value) => format!("r:{:x}", value.to_bits()),
        SqlValue::Text(value) => format!("t:{value}"),
    }
}

fn show_canons(canons: &[Canon]) -> String {
    canons
        .iter()
        .map(|canon| {
            format!(
                "cols={:?} changes={} last={} rows={:?}",
                canon.columns, canon.changes, canon.last_row_id, canon.rows
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_osd(worker: &SqliteWorker, sql: &str, params: &[SqlValue]) -> Result<Vec<Canon>, String> {
    let response = worker
        .execute_blocking(SqlRequest {
            sql: sql.to_string(),
            params: params.to_vec(),
            shape: ResultShape::Raw,
        })
        .map_err(|error| error.to_string())?;
    Ok(response
        .statements
        .iter()
        .map(|statement| Canon {
            columns: statement.columns.iter().cloned().collect(),
            rows: statement
                .rows
                .iter()
                .map(|row| row.iter().map(canon_value).collect())
                .collect(),
            changes: statement.meta.changes,
            last_row_id: statement.meta.last_row_id,
        })
        .collect())
}

fn open_reference(path: &Path) -> Connection {
    let conn = Connection::open(path).expect("reference db");
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        PRAGMA foreign_keys=ON;
        PRAGMA busy_timeout=0;
        ",
    )
    .expect("reference pragmas");
    conn
}

fn run_ref(conn: &Connection, generated: &Generated) -> Result<Vec<Canon>, String> {
    if !conn.is_autocommit() {
        conn.execute_batch("ROLLBACK").ok();
    }
    conn.execute_batch("BEGIN IMMEDIATE")
        .map_err(|error| error.to_string())?;
    let outcome = if generated.script {
        run_ref_batch(conn, &generated.sql)
    } else {
        run_ref_one(conn, &generated.sql, &generated.params)
    };
    match outcome {
        Ok(canons) => {
            if let Err(error) = conn.execute_batch("COMMIT") {
                conn.execute_batch("ROLLBACK").ok();
                return Err(error.to_string());
            }
            Ok(canons)
        }
        Err(error) => {
            conn.execute_batch("ROLLBACK").ok();
            Err(error)
        }
    }
}

fn run_ref_one(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<Vec<Canon>, String> {
    let mut stmt = conn.prepare(sql).map_err(|error| error.to_string())?;
    Ok(vec![read_stmt(conn, &mut stmt, params)?])
}

fn run_ref_batch(conn: &Connection, sql: &str) -> Result<Vec<Canon>, String> {
    let mut batch = Batch::new(conn, sql);
    let mut canons = Vec::new();
    loop {
        let mut stmt = match batch.next() {
            Ok(Some(stmt)) => stmt,
            Ok(None) => break,
            Err(error) => return Err(error.to_string()),
        };
        canons.push(read_stmt(conn, &mut stmt, &[])?);
    }
    if canons.is_empty() {
        return Err("empty sql".to_string());
    }
    Ok(canons)
}

fn read_stmt(
    conn: &Connection,
    stmt: &mut rusqlite::Statement<'_>,
    params: &[SqlValue],
) -> Result<Canon, String> {
    let before = conn.total_changes();
    let bound: Vec<rusqlite::types::Value> = params
        .iter()
        .map(|value| match value {
            SqlValue::Null => rusqlite::types::Value::Null,
            SqlValue::Integer(value) => rusqlite::types::Value::Integer(*value),
            SqlValue::Real(value) => rusqlite::types::Value::Real(*value),
            SqlValue::Text(value) => rusqlite::types::Value::Text(value.clone()),
        })
        .collect();
    let columns: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut rows_iter = stmt
        .query(params_from_iter(bound))
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    loop {
        let next = rows_iter.next().map_err(|error| error.to_string())?;
        let Some(row) = next else { break };
        let mut values = Vec::new();
        for index in 0..columns.len() {
            let value = row.get_ref(index).map_err(|error| error.to_string())?;
            values.push(canon_value(&sql_from_ref(value)?));
        }
        rows.push(values);
    }
    Ok(Canon {
        columns,
        rows,
        changes: conn.total_changes().saturating_sub(before) as u64,
        last_row_id: conn.last_insert_rowid(),
    })
}

fn sql_from_ref(value: ValueRef<'_>) -> Result<SqlValue, String> {
    match value {
        ValueRef::Null => Ok(SqlValue::Null),
        ValueRef::Integer(value) => Ok(SqlValue::Integer(value)),
        ValueRef::Real(value) => Ok(SqlValue::Real(value)),
        ValueRef::Text(value) => Ok(SqlValue::Text(String::from_utf8_lossy(value).into_owned())),
        ValueRef::Blob(_) => Err("blob results are not supported".to_string()),
    }
}

fn query_text(canons: &[Canon]) -> Vec<Vec<String>> {
    canons
        .first()
        .map(|canon| canon.rows.clone())
        .unwrap_or_default()
}

/// Snapshot queries use `quote()`, so each cell is text. Canon form prefixes that text with `t:`.
fn plain_text(canons: &[Canon]) -> Vec<Vec<String>> {
    query_text(canons)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.strip_prefix("t:").map(str::to_string).unwrap_or(cell))
                .collect()
        })
        .collect()
}

fn snapshot_sql(
    query: &mut dyn FnMut(&str) -> Result<Vec<Vec<String>>, String>,
) -> Result<String, String> {
    let master = query(
        "SELECT type, name, COALESCE(sql, '') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY 1, 2, 3",
    )?;
    let mut out = String::new();
    for row in &master {
        out.push_str(&row.join("|"));
        out.push('\n');
        if row.first().map(String::as_str) != Some("table") {
            continue;
        }
        let name = &row[1];
        let cols = query(&format!(
            "SELECT name FROM pragma_table_info('{name}') ORDER BY cid"
        ))?;
        let quoted = cols
            .iter()
            .filter_map(|col| col.first())
            .map(|col| format!("quote(\"{col}\")"))
            .collect::<Vec<_>>();
        let list = if quoted.is_empty() {
            "quote(rowid)".to_string()
        } else {
            format!("quote(rowid), {}", quoted.join(", "))
        };
        let data = query(&format!("SELECT {list} FROM \"{name}\" ORDER BY rowid"))?;
        for data_row in data {
            out.push_str("  ");
            out.push_str(&data_row.join("|"));
            out.push('\n');
        }
    }
    for row in &master {
        if row.first().map(String::as_str) != Some("view") {
            continue;
        }
        let name = &row[1];
        // DROP TABLE leaves the view in sqlite_master. pragma_table_info then
        // fails because the view's table is gone. Both engines do this; record
        // it and keep comparing the rest of the dump.
        let cols = match query(&format!(
            "SELECT name FROM pragma_table_info('{name}') ORDER BY cid"
        )) {
            Ok(cols) => cols,
            Err(_) => {
                out.push_str("  view error\n");
                continue;
            }
        };
        let quoted = cols
            .iter()
            .filter_map(|col| col.first())
            .map(|col| format!("quote(\"{col}\")"))
            .collect::<Vec<_>>();
        if quoted.is_empty() {
            out.push_str("  view error\n");
            continue;
        }
        let order = (1..=quoted.len())
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        match query(&format!(
            "SELECT {} FROM \"{name}\" ORDER BY {order}",
            quoted.join(", ")
        )) {
            Ok(data) => {
                for data_row in data {
                    out.push_str("  view ");
                    out.push_str(&data_row.join("|"));
                    out.push('\n');
                }
            }
            Err(_) => {
                out.push_str("  view error\n");
            }
        }
    }
    Ok(out)
}

fn snapshot_osd(worker: &SqliteWorker) -> Result<String, String> {
    snapshot_sql(&mut |sql| {
        let canons =
            run_osd(worker, sql, &[]).map_err(|error| format!("{error}\nwhile reading {sql}"))?;
        Ok(plain_text(&canons))
    })
}

fn snapshot_ref(conn: &Connection) -> Result<String, String> {
    snapshot_sql(&mut |sql| {
        let generated = Generated {
            sql: sql.to_string(),
            params: Vec::new(),
            script: false,
            denied: false,
            expect_err: false,
            effect: Effect::None,
        };
        Ok(plain_text(&run_ref(conn, &generated).map_err(|error| {
            format!("{error}\nwhile reading {sql}")
        })?))
    })
}

struct Fail {
    seed: u64,
    step: usize,
    sql: String,
    params: Vec<SqlValue>,
}

fn fail(ctx: &Fail, osd_snap: &str, ref_snap: &str, message: String) -> ! {
    panic!(
        "fuzz seed {} step {}\nsql: {}\nparams: {:?}\n{message}\n--- osd1 ---\n{osd_snap}\n--- reference ---\n{ref_snap}",
        ctx.seed, ctx.step, ctx.sql, ctx.params
    );
}

fn temp_path(seed: u64, which: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "osd1-fuzz-{}-{n}-{seed}-{which}.db",
        std::process::id()
    ))
}

fn remove_db(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let candidate = if suffix.is_empty() {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = std::fs::remove_file(candidate);
    }
}

fn setup_audit() -> Generated {
    ok(
        "CREATE TABLE audit (id INTEGER PRIMARY KEY, src TEXT, n INTEGER)".to_string(),
        Vec::new(),
        false,
        Effect::AddTable(Table {
            name: "audit".to_string(),
            cols: vec![
                Col {
                    name: "id".to_string(),
                    kind: ColKind::Int,
                    pk: true,
                    notnull: false,
                    unique: false,
                    references: None,
                    cascade: false,
                },
                Col {
                    name: "src".to_string(),
                    kind: ColKind::Text,
                    pk: false,
                    notnull: false,
                    unique: false,
                    references: None,
                    cascade: false,
                },
                Col {
                    name: "n".to_string(),
                    kind: ColKind::Int,
                    pk: false,
                    notnull: false,
                    unique: false,
                    references: None,
                    cascade: false,
                },
            ],
            next_col: 3,
            pinned: true,
        }),
    )
}

fn run_seed(seed: u64, steps: usize) {
    let osd_path = temp_path(seed, "osd");
    let ref_path = temp_path(seed, "ref");
    let worker = SqliteWorker::open(&osd_path, 8, "LOC").expect("osd1 db");
    let reference = open_reference(&ref_path);
    let mut rng = Rng::new(seed);
    let mut model = Model::new();
    let mut step = 0usize;
    let mut upcoming = Some(setup_audit());
    // Drop table is part of the mix but rare; fold it into generate by swapping
    // a denied roll sometimes below.
    for _ in 0..steps {
        let mut generated = upcoming.take().unwrap_or_else(|| model.generate(&mut rng));
        if generated.denied && rng.bool(3) {
            if let Some(drop) = model.drop_table_op(&mut rng) {
                generated = drop;
            }
        }
        let ctx = Fail {
            seed,
            step,
            sql: generated.sql.clone(),
            params: generated.params.clone(),
        };
        let osd = run_osd(&worker, &generated.sql, &generated.params);
        let reference_result = if generated.denied {
            None
        } else {
            Some(run_ref(&reference, &generated))
        };
        let osd_snap =
            snapshot_osd(&worker).unwrap_or_else(|error| format!("snapshot failed: {error}"));
        let ref_snap =
            snapshot_ref(&reference).unwrap_or_else(|error| format!("snapshot failed: {error}"));
        if osd_snap.starts_with("snapshot failed") || ref_snap.starts_with("snapshot failed") {
            fail(&ctx, &osd_snap, &ref_snap, "snapshot failed".to_string());
        }
        if generated.denied {
            if osd.is_ok() {
                fail(
                    &ctx,
                    &osd_snap,
                    &ref_snap,
                    "expected this statement to be rejected".to_string(),
                );
            }
            if osd_snap != ref_snap {
                fail(
                    &ctx,
                    &osd_snap,
                    &ref_snap,
                    "rejected statement changed the database".to_string(),
                );
            }
        } else if generated.expect_err {
            match (&osd, reference_result.as_ref()) {
                (Err(_), Some(Err(_))) => {}
                _ => fail(
                    &ctx,
                    &osd_snap,
                    &ref_snap,
                    format!(
                        "expected both sides to fail\nosd1={osd:?}\nreference={reference_result:?}"
                    ),
                ),
            }
            if osd_snap != ref_snap {
                fail(
                    &ctx,
                    &osd_snap,
                    &ref_snap,
                    "failed statement left different databases".to_string(),
                );
            }
        } else {
            match (osd, reference_result) {
                (Ok(left), Some(Ok(right))) => {
                    if left != right {
                        fail(
                            &ctx,
                            &osd_snap,
                            &ref_snap,
                            format!(
                                "results differ\nosd1:\n{}\nreference:\n{}",
                                show_canons(&left),
                                show_canons(&right)
                            ),
                        );
                    }
                    model.apply(generated.effect);
                }
                (Err(_), Some(Err(_))) => {
                    if osd_snap != ref_snap {
                        fail(
                            &ctx,
                            &osd_snap,
                            &ref_snap,
                            "both failed but the databases differ".to_string(),
                        );
                    }
                }
                (left, right) => fail(
                    &ctx,
                    &osd_snap,
                    &ref_snap,
                    format!("one side failed\nosd1={left:?}\nreference={right:?}"),
                ),
            }
            if osd_snap != ref_snap {
                fail(
                    &ctx,
                    &osd_snap,
                    &ref_snap,
                    "databases differ after a successful statement".to_string(),
                );
            }
        }
        step += 1;
    }
    let integrity = run_osd(&worker, "PRAGMA integrity_check", &[]).expect("integrity");
    let ref_integrity = run_ref(
        &reference,
        &ok(
            "PRAGMA integrity_check".to_string(),
            Vec::new(),
            false,
            Effect::None,
        ),
    )
    .expect("reference integrity");
    assert_eq!(integrity, ref_integrity, "seed {seed} integrity differs");
    assert_eq!(
        integrity
            .first()
            .and_then(|canon| canon.rows.first())
            .and_then(|row| row.first())
            .map(String::as_str),
        Some("t:ok"),
        "seed {seed} integrity was not ok: {integrity:?}"
    );
    drop(worker);
    drop(reference);
    remove_db(&osd_path);
    remove_db(&ref_path);
}

fn cases() -> Vec<(u64, usize)> {
    if let Ok(seed) = std::env::var("OSD1_FUZZ_SEED") {
        let seed: u64 = seed.parse().expect("OSD1_FUZZ_SEED is a number");
        let steps = std::env::var("OSD1_FUZZ_STEPS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_000);
        vec![(seed, steps)]
    } else {
        vec![(1, 200), (7, 200), (13, 200), (42, 200)]
    }
}

#[test]
fn fuzzy_mix_matches_reference_sqlite() {
    for (seed, steps) in cases() {
        run_seed(seed, steps);
    }
}
