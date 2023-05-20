//! SQL serialization

use serde::Serializer;

use crate::error::Error;

/// SQL serializer
#[derive(Debug, Clone)]
pub struct SqlSerializer;

impl SqlSerializer {
    /// Creates a new SQL serializer
    pub fn new() -> Self {
        SqlSerializer
    }
}

impl Default for SqlSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// SQL serialization sequence
#[derive(Debug, Default)]
pub struct SqlSerializeSeq {
    values: Vec<String>,
}

impl serde::ser::SerializeSeq for SqlSerializeSeq {
    type Ok = String;

    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let v = value.serialize(sql_serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(format!("[{}]", self.values.join(", ")))
    }
}

/// SQL serialization tuple
#[derive(Debug, Default)]
pub struct SqlSerializeTuple {
    values: Vec<String>,
}

impl serde::ser::SerializeTuple for SqlSerializeTuple {
    type Ok = String;

    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let v = value.serialize(sql_serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(format!("({})", self.values.join(", ")))
    }
}

/// SQL serialization tuple
#[derive(Debug, Default)]
pub struct SqlSerializeTupleStruct {
    values: Vec<String>,
}

impl serde::ser::SerializeTupleStruct for SqlSerializeTupleStruct {
    type Ok = String;

    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let v = value.serialize(sql_serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(format!("({})", self.values.join(", ")))
    }
}

/// SQL serialization tuple
#[derive(Debug, Default)]
pub struct SqlSerializeTupleVariant {
    values: Vec<String>,
}

impl serde::ser::SerializeTupleVariant for SqlSerializeTupleVariant {
    type Ok = String;

    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let v = value.serialize(sql_serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(format!("({})", self.values.join(", ")))
    }
}

/// SQL serialization map
#[derive(Debug, Default)]
pub struct SqlSerializeMap {
    keys: Vec<String>,
    values: Vec<String>,
}

impl serde::ser::SerializeMap for SqlSerializeMap {
    type Ok = String;

    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let v = key.serialize(sql_serializer)?;
        self.keys.push(v);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let v = value.serialize(sql_serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(format!(
            "{{{}}}",
            self.keys
                .iter()
                .enumerate()
                .map(|(i, key)| { format!("{}: {}", key, self.values.get(i).unwrap()) })
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

/// SQL serialization struct
#[derive(Debug, Default)]
pub struct SqlSerializeStruct {
    values: Vec<String>,
}

impl serde::ser::SerializeStruct for SqlSerializeStruct {
    type Ok = String;

    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let sql_serializer = SqlSerializer::new();
        let key = sql_serializer.clone().serialize_str(key)?;
        let value = value.serialize(sql_serializer)?;
        let kv = format!("{}: {}", key, value);
        self.values.push(kv);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{{{}}}", self.values.join(", ")))
    }
}

/// SQL serialization struct
#[derive(Debug, Default)]
pub struct SqlSerializeStructVariant {
    _values: Vec<String>,
}

impl serde::ser::SerializeStructVariant for SqlSerializeStructVariant {
    type Ok = String;

    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl Serializer for SqlSerializer {
    type Ok = String;

    type Error = Error;

    type SerializeSeq = SqlSerializeSeq;

    type SerializeTuple = SqlSerializeTuple;

    type SerializeTupleStruct = SqlSerializeTupleStruct;

    type SerializeTupleVariant = SqlSerializeTupleVariant;

    type SerializeMap = SqlSerializeMap;

    type SerializeStruct = SqlSerializeStruct;

    type SerializeStructVariant = SqlSerializeStructVariant;

    fn is_human_readable(&self) -> bool {
        true
    }

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        if v {
            Ok(1.to_string())
        } else {
            Ok(0.to_string())
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(format!("'{}'", v))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let s = v.replace('\'', "\\\'");
        Ok(format!("'{}'", s))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::new("bytes is not a valid SQL type"))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok("NULL".to_string())
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::new("unit type () is not a valid SQL type"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::new("unit struct is not a valid SQL type"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SqlSerializeSeq::default())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(SqlSerializeTuple::default())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(SqlSerializeTupleStruct::default())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SqlSerializeTupleVariant::default())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SqlSerializeMap::default())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(SqlSerializeStruct::default())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SqlSerializeStructVariant::default())
    }
}
