use sqlx::mysql::MySqlArguments;
use sqlx::{Arguments, Encode, MySql, Type};

#[derive(Default, Clone)]
pub struct UpdateFieldArgsBuilder {
    fields: Vec<String>,
    args:   MySqlArguments,
}

impl UpdateFieldArgsBuilder {
    pub fn add<'q, T>(&mut self, k: &'q str, v: T)
    where
        T: Encode<'q, MySql> + Type<MySql> + Send,
        T: 'q,
    {
        self.fields.push(format!("{}=?", k));
        self.args.add(v);
    }

    pub fn add_opt<'q, T>(&mut self, k: &'q str, v: &'q Option<T>)
    where
        T: Encode<'q, MySql> + Type<MySql> + Sync + Send,
    {
        if let Some(v) = v {
            self.fields.push(format!("{}=?", k));
            self.args.add(v);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn str_args(&self) -> (String, MySqlArguments) {
        (self.fields.join(","), self.args.clone())
    }
}

#[derive(Default, Clone)]
pub struct InsertSqlArgsBuilder<'a> {
    table_name:   &'a str,
    fields:       Vec<&'a str>,
    placeholders: Vec<&'a str>,
    args:         MySqlArguments,
}

impl<'a> InsertSqlArgsBuilder<'a> {
    pub fn new(table_name: &str) -> InsertSqlArgsBuilder {
        InsertSqlArgsBuilder {
            table_name,
            fields: Default::default(),
            placeholders: Default::default(),
            args: Default::default(),
        }
    }

    pub fn add_opt<'q, T>(&mut self, k: &'a str, v: &'q Option<T>)
    where
        T: Encode<'q, MySql> + Type<MySql> + Sync + Send,
    {
        if let Some(v) = v {
            self.fields.push(k);
            self.placeholders.push("?");
            self.args.add(v);
        }
    }

    pub fn add<'q, T>(&mut self, k: &'a str, v: T)
    where
        T: Encode<'q, MySql> + Type<MySql> + Send,
        T: 'q,
    {
        self.fields.push(k);
        self.placeholders.push("?");
        self.args.add(v);
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn str_args(&self) -> (String, String, MySqlArguments) {
        (
            self.fields.join(","),
            self.placeholders.join(","),
            self.args.clone(),
        )
    }

    pub fn insert_sql_args(self) -> (String, MySqlArguments) {
        let sql = format!(
            "INSERT INTO {}({}) VALUES ({})",
            self.table_name,
            self.fields.join(","),
            self.placeholders.join(",")
        );
        (sql, self.args)
    }

    pub fn replace_sql_args(self) -> (String, MySqlArguments) {
        let sql = format!(
            "REPLACE INTO {}({}) VALUES ({})",
            self.table_name,
            self.fields.join(","),
            self.placeholders.join(",")
        );
        (sql, self.args)
    }
}

#[derive(Default, Clone)]
pub struct WhereArgsBuilder {
    fields: Vec<String>,
    args:   MySqlArguments,
}

impl WhereArgsBuilder {
    pub fn new_with_args(args: MySqlArguments) -> Self {
        WhereArgsBuilder {
            fields: Vec::new(),
            args,
        }
    }

    pub fn add_str(&mut self, where_str: &str) {
        self.fields.push(where_str.to_string())
    }

    pub fn add_combine<'q, T>(&mut self, where_str: &str, v: T)
    where
        T: Encode<'q, MySql> + Type<MySql>,
        T: 'q + Send,
    {
        self.fields.push(where_str.to_string());
        self.args.add(v)
    }

    pub fn add<'q, T>(&mut self, k: &str, v: T)
    where
        T: Encode<'q, MySql> + Type<MySql>,
        T: 'q + Send,
    {
        self.fields.push(format!("{}=?", k));
        self.args.add(v);
    }

    pub fn add_opt<'q, T>(&mut self, k: &'q str, v: &'q Option<T>)
    where
        T: Encode<'q, MySql> + Type<MySql> + Sync + Send,
    {
        if let Some(v) = v {
            self.fields.push(format!("{}=?", k));
            self.args.add(v);
        }
    }

    pub fn str_args(&self) -> (String, MySqlArguments) {
        if self.fields.is_empty() {
            ("".to_string(), self.args.clone())
        } else {
            (
                format!("WHERE {}", self.fields.join(" AND ")),
                self.args.clone(),
            )
        }
    }
}
