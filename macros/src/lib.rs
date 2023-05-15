//! Macros for `clickhouse-client`
//!
//! # [derive(DbRow)]
//!
//! This macro parses a struct and implements the trait `DbRowExt

use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error, OptionExt};
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Field, Ident, ItemStruct, LitStr};

/// A macro to derive the trait `DbRowExt`
///
/// # Prerequisites
///
/// - each field Rust type must implement the trait `DbType` and `DbValue`
/// - The following elements must be in scope: `DbRowExt`, `DbType`, `DbValue`, `TableSchema`, `ColumnSchema`
///
/// # Attributes
///
/// This macro accepts struct and field level attribute called `db`.
///
/// ## Struct level attributes:
/// - **table**: table name (mandatory)
///
/// ## Field level attributes:
/// - **name**: column name (optional)
/// - **primary**: indicates a primary key (optional)
/// - **skip**: field is skipped (optional)
///
/// # Example
///
/// ```ignore
/// #[derive(DbRow)]
/// #[db(table = "my_table")]
/// struct MyRecord {
///   #[db(primary)]
///   id: u32,
///   #[db(name = "id")]
///   id: u32,
///   #[db(skip)]
///   other: String,
/// }
/// ```
#[proc_macro_error]
#[proc_macro_derive(DbRow, attributes(db))]
pub fn derive_db_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let ident = &input.ident;
    let fields = &input.fields;
    // let attrs = &input.attrs;
    // let vis = &input.vis;
    // let generics = &input.generics;
    // let semi_token = &input.semi_token;

    // parse struct attributes
    let struct_attrs = StructAttrs::parse("db", &input);
    // eprintln!("struct_attrs= {:#?}", struct_attrs);
    let table_name = struct_attrs.table_name;

    // parse fields
    let mut table_cols = vec![];
    let mut map_insert_values = vec![];
    let mut has_primary_key = false;
    let mut set_record_fields = vec![];

    for field in fields {
        let field_attrs = FieldAttrs::parse("db", field);
        // eprintln!("field_attrs= {:#?}", field_attrs);

        let field_id = &field_attrs.field_id;
        let field_type = &field.ty;
        let col_name = &field_attrs.col_name;
        let col_is_primary = &field_attrs.primary;
        if *col_is_primary {
            has_primary_key = true;
        }

        if !field_attrs.skip {
            table_cols.push(quote! {
                .column(ColumnSchema {
                    name: #col_name.to_string(),
                    ty: <#field_type as DbType>::TYPE.to_string(),
                    is_primary: #col_is_primary,
                })
            });
            map_insert_values.push(quote! {
                map.insert(#col_name, Box::new(&self.#field_id));
            });
            set_record_fields.push(quote! {
                record.#field_id = <#field_type as DbValue>::from_sql_str(values[#col_name])
                .map_err(|err| format!("({}) {}", #col_name, err))?;
            });
        }
    }

    // check that there is at least 1 primary key
    if !has_primary_key {
        abort!(input.span(), "There must at least 1 primary key");
    }

    quote! {
        impl DbRowExt for #ident {
            fn db_schema() -> TableSchema {
                TableSchema::new(#table_name)
                #(#table_cols)*
            }

            fn db_values(&self) -> ::std::collections::HashMap<&'static str, Box<&'_ dyn DbValue>> {
                // NB: map must be typed, otherwise it infers the value type from the 1st inserted value
                let mut map: ::std::collections::HashMap<&str, Box<&'_ dyn DbValue>> = ::std::collections::HashMap::new();
                #(#map_insert_values) *
                map
            }

            fn from_db_values(values: ::std::collections::HashMap<&str, &str>) -> ::std::result::Result<Self, String>
            where
                Self: Sized + Default
            {
                let mut record = Self::default();
                #(#set_record_fields) *
                Ok(record)
            }
        }
    }
    .into()
}

/// Struct attributes
struct StructAttrs {
    table_name: LitStr,
}

impl StructAttrs {
    /// Parses the struct attribute
    fn parse(attr_key: &str, item: &ItemStruct) -> Self {
        let mut table_name: Option<LitStr> = None;

        let attrs = &item.attrs;
        for attr in attrs.iter() {
            // eprintln!("ATTR: {:#?}", attr);
            match &attr.meta {
                syn::Meta::Path(_) => continue,
                syn::Meta::NameValue(_) => continue,
                syn::Meta::List(list) => {
                    if list.path.is_ident(attr_key) {
                        let tokens = list.tokens.to_string();
                        for part in tokens.split(',') {
                            match part.trim().split_once('=') {
                                Some((key, val)) => {
                                    let key = key.trim();
                                    let val = val.trim();
                                    // eprintln!("XXXX val={:?}", val);
                                    if val.is_empty() {
                                        list.tokens.span();
                                        abort!(list.tokens.span(), "missing value");
                                    }

                                    match key {
                                        "table" => {
                                            let val_lit = match syn::parse_str::<LitStr>(val) {
                                                Ok(ok) => ok,
                                                Err(_) => {
                                                    abort!(
                                                        list.tokens.span(),
                                                        "value must be quoted"
                                                    );
                                                }
                                            };
                                            if val_lit.value().is_empty() {
                                                abort!(list.tokens.span(), "value is empty");
                                            }
                                            table_name = Some(val_lit);
                                        }
                                        _ => {
                                            abort!(
                                                list.tokens.span(),
                                                "invalid key (valid: table)"
                                            );
                                        }
                                    }
                                }
                                None => {
                                    abort!(
                                        list.tokens.span(),
                                        "invalid attribute (must be key=\"val\")"
                                    );
                                }
                            };
                        }
                    }
                }
            }
        }

        Self {
            table_name: table_name.expect_or_abort("missing attribute 'table'"),
        }
    }
}

/// Field attributes
struct FieldAttrs {
    field_id: Ident,
    col_name: LitStr,
    skip: bool,
    primary: bool,
}

impl FieldAttrs {
    /// Parses a field
    fn parse(attr_key: &str, field: &Field) -> Self {
        let field_id = field.ident.clone().expect_or_abort("missing struct field");
        let mut col_name = LitStr::new(field_id.to_string().as_str(), field.span());
        let mut skip = false;
        let mut primary = false;

        for attr in field.attrs.iter() {
            // eprintln!("ATTR: {:#?}", attr);
            match &attr.meta {
                syn::Meta::Path(_) => continue,
                syn::Meta::NameValue(_) => continue,
                syn::Meta::List(list) => {
                    if list.path.is_ident(attr_key) {
                        let tokens = list.tokens.to_string();
                        for part in tokens.split(',') {
                            match part {
                                "skip" => {
                                    skip = true;
                                    continue;
                                }
                                "primary" => {
                                    primary = true;
                                    continue;
                                }
                                _ => {}
                            }

                            match part.trim().split_once('=') {
                                Some((key, val)) => {
                                    let key = key.trim();
                                    let val = val.trim();
                                    // eprintln!("YYYY val={:?}", val);
                                    if val.is_empty() {
                                        list.tokens.span();
                                        abort!(list.tokens.span(), "missing value");
                                    }

                                    match key {
                                        "name" => {
                                            let val_lit = match syn::parse_str::<LitStr>(val) {
                                                Ok(ok) => ok,
                                                Err(_) => {
                                                    abort!(
                                                        list.tokens.span(),
                                                        "value must be quoted"
                                                    );
                                                }
                                            };
                                            if val_lit.value().is_empty() {
                                                abort!(list.tokens.span(), "value is empty");
                                            }
                                            col_name = val_lit;
                                        }
                                        _ => {
                                            abort!(list.tokens.span(), "invalid key (valid: name)");
                                        }
                                    }
                                }
                                None => {
                                    abort!(
                                        list.tokens.span(),
                                        "invalid attribute (must be key=\"val\")"
                                    );
                                }
                            };
                        }
                    }
                }
            }
        }

        // end of 'db' attribute
        Self {
            field_id,
            col_name,
            skip,
            primary,
        }
    }
}
