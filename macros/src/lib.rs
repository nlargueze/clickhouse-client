//! Macros for `clickhouse-client`
//!
//! # [derive(AsChRecord)]
//!
//! This macro parses a struct and implements the trait `clickhouse-client::orm::ChRecord`

use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error, OptionExt};
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Field, Ident, ItemStruct, LitBool, LitStr};

/// A macro to derive the trait `ChRecord`
///
/// # Prerequisites
///
/// - each field type must implement the trait `ChValue` to map to a DB type
/// - The following types must be in scope:
///     - `Record`
///     - `ChValue`
///     - `Value
///     - `Type`
///     - `TableSchema`
///     - `Error`
///
/// # Attributes
///
/// This macro accepts struct and field level attribute called `ch`.
///
/// ## Struct level attributes:
/// - **table**: table name (mandatory)
///
/// ## Field level attributes:
/// - **name**: column name (optional)
/// - **primary_key**: indicates a primary key (optional)
/// - **skip**: field is skipped (optional)
///
/// # Example
///
/// ```ignore
/// #[derive(AsChRecord)]
/// #[ch(table = "my_table")]
/// struct MyRecord {
///   #[ch(primary_key)]
///   id: u32,
///   #[ch(name = "id")]
///   id: u32,
///   #[ch(skip)]
///   other: String,
/// }
/// ```
#[proc_macro_error]
#[proc_macro_derive(AsChRecord, attributes(ch))]
pub fn derive_db_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let ident = &input.ident;
    let fields = &input.fields;
    // let attrs = &input.attrs;
    // let vis = &input.vis;
    // let generics = &input.generics;
    // let semi_token = &input.semi_token;

    // parse struct attributes
    let attrs = StructAttrs::parse("ch", &input);
    // eprintln!("struct_attrs= {:#?}", struct_attrs);
    let table_name = attrs.table_name;

    // parse fields
    let mut schema_entries = vec![];
    let mut into_record_entries = vec![];
    let mut from_record_entries = vec![];

    let mut has_primary_key = false;
    for field in fields {
        let attrs = FieldAttrs::parse("ch", field);
        // eprintln!("field_attrs= {:#?}", field_attrs);

        let field_id = &attrs.field_id;
        let field_type = &field.ty;
        let col_name = &attrs.col_name;
        let col_primary = &attrs.primary;

        if !attrs.skip.value {
            // .column("id", Uuid::ch_type(), true)
            schema_entries.push(quote! {
                .column(#col_name, #field_type::ch_type(), #col_primary)
            });

            // .field("id", true, Uuid::ch_type(), self.id.into_ch_value())
            into_record_entries.push(quote! {
                .field(#col_name, #col_primary, #field_type::ch_type(), self.#field_id.into_ch_value())
            });

            // id: match record.remove_field("id") {
            //     Some(field) => ::uuid::Uuid::from_ch_value(field.value)?,
            //     None => return Err(Error::new("Missing field 'id'")),
            // },
            from_record_entries.push(quote! {
                #field_id: match record.remove_field(#col_name) {
                    Some(field) => #field_type::from_ch_value(field.value)?,
                    None => return Err(Error::new(format!("Missing field '{}'", #col_name).as_str())),
                }
            });

            if col_primary.value {
                has_primary_key = true;
            }
        }
    }

    // check that there is at least 1 primary key
    if !has_primary_key {
        abort!(input.span(), "There must at least 1 primary key");
    }

    quote! {
        impl ChRecord for #ident {
            fn ch_schema() -> TableSchema {
                TableSchema::new(#table_name)
                    #(#schema_entries)*
            }

            fn into_ch_record(self) -> Record {
                Record::new(#table_name)
                    #(#into_record_entries)*
            }

            fn from_ch_record(mut record: Record) -> Result<Self, Error> {
                Ok(Self {
                    #(#from_record_entries),*
                })
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
    skip: LitBool,
    primary: LitBool,
}

impl FieldAttrs {
    /// Parses a field
    fn parse(attr_key: &str, field: &Field) -> Self {
        let field_id = field.ident.clone().expect_or_abort("missing struct field");
        let mut col_name = LitStr::new(field_id.to_string().as_str(), field.span());
        let mut skip = LitBool::new(false, field.span());
        let mut primary = LitBool::new(false, field.span());

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
                                    skip = LitBool::new(true, list.tokens.span());
                                    continue;
                                }
                                "primary_key" => {
                                    primary = LitBool::new(true, list.tokens.span());
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
