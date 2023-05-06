//! Macros for `clickhouse-client`
//!
//! # [derive(DbRow)]
//!
//! This macro parses a struct and implements the macro `IsDbRow`.
//!
//! The following elements must be in scope: `IsDbRow`, `DbType`
//!
//! # Example
//!
//! ```ignore
//! #[derive(DbRow)]
//! #[db(table = "my_table")]
//! struct MyRecord {
//!   #[db(name = "id", type = "UInt32")]
//!   id: u32,
//! }
//! ```

use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error, OptionExt};
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Field, Ident, ItemStruct, LitStr};

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
    let struct_attrs = parse_struct("db", &input);
    // eprintln!("struct_attrs= {:#?}", struct_attrs);
    let table_name = struct_attrs.table_name;

    // parse fields
    let mut columns = vec![];
    let mut inserts = vec![];
    for field in fields {
        let field_attrs = parse_field("db", field);
        // eprintln!("field_attrs= {:#?}", field_attrs);
        if !field_attrs.skip {
            columns.push(field_attrs.col_name.clone());
            inserts.push(field_attrs.insert_expr());
        }
    }

    quote! {
        impl IsDbRow for #ident {
            fn table() -> &'static str {
                #table_name
            }

            fn columns() -> Vec<&'static str> {
                vec![#(#columns,)*]
            }

            fn values(&self) -> ::std::collections::HashMap<&'static str, Box<dyn DbType + '_>> {
                // NB: map must be typed, otherwise it infers the value type from the 1st inserted value
                let mut map: ::std::collections::HashMap<&str, Box<dyn DbType>> = ::std::collections::HashMap::new();
                #(#inserts) *
                map
            }
        }
    }
    .into()
}

impl FieldAttrs {
    /// Returns the expression to inert the field into a HashMap called 'map'
    ///
    /// Assumptions:
    /// - map is called `map` and is a HashMap
    /// - the field type must implement the trait `DbType`
    fn insert_expr(&self) -> proc_macro2::TokenStream {
        let field_id = &self.field_id;
        let col_name = &self.col_name;

        quote! {
            map.insert(#col_name, Box::new(&self.#field_id));
        }
    }
}

/// Struct attributes
#[derive(Debug)]
struct StructAttrs {
    table_name: LitStr,
}

/// Parses the struct attribute
fn parse_struct(attr_key: &str, item: &ItemStruct) -> StructAttrs {
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
                                                abort!(list.tokens.span(), "value must be quoted");
                                            }
                                        };
                                        if val_lit.value().is_empty() {
                                            abort!(list.tokens.span(), "value is empty");
                                        }
                                        table_name = Some(val_lit);
                                    }
                                    _ => {
                                        abort!(list.tokens.span(), "invalid key (valid: table)");
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

    StructAttrs {
        table_name: table_name.expect_or_abort("missing attribute 'table'"),
    }
}

/// Field attributes
#[derive(Debug)]
struct FieldAttrs {
    field_id: Ident,
    col_name: LitStr,
    skip: bool,
}

/// Parses a field
fn parse_field(attr_key: &str, field: &Field) -> FieldAttrs {
    let field_id = field.ident.clone().expect_or_abort("missing struct field");
    let mut col_name = LitStr::new(field_id.to_string().as_str(), field.span());
    let mut skip = false;

    for attr in field.attrs.iter() {
        // eprintln!("ATTR: {:#?}", attr);
        match &attr.meta {
            syn::Meta::Path(_) => continue,
            syn::Meta::NameValue(_) => continue,
            syn::Meta::List(list) => {
                if list.path.is_ident(attr_key) {
                    let tokens = list.tokens.to_string();
                    for part in tokens.split(',') {
                        if part == "skip" {
                            skip = true;
                            continue;
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
                                                abort!(list.tokens.span(), "value must be quoted");
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

    FieldAttrs {
        field_id,
        col_name,
        skip,
    }
}
