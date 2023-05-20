//! RowBinary serialization

use std::io::Write;

use serde::{Serialize, Serializer};

use crate::error::Error;

/// Serializer for the `RowBin` format
#[derive(Debug, Default, Clone, Copy)]
pub struct RowBinSerializer;

impl RowBinSerializer {
    /// Creates a new RowBinary formatter
    pub fn new() -> Self {
        Self::default()
    }
}

/// RowBin serialization sequence
#[derive(Debug, Default)]
pub struct RowBinSerializeSeq {
    values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeSeq for RowBinSerializeSeq {
    type Ok = Vec<u8>;

    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let v = value.serialize(serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.values.concat())
    }
}

/// RowBin serialization tuple
#[derive(Debug, Default)]
pub struct RowBinSerializeTuple {
    values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeTuple for RowBinSerializeTuple {
    type Ok = Vec<u8>;

    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let v = value.serialize(serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.values.concat())
    }
}

/// RowBin serialization tuple
#[derive(Debug, Default)]
pub struct RowBinSerializeTupleStruct {
    values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeTupleStruct for RowBinSerializeTupleStruct {
    type Ok = Vec<u8>;

    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let v = value.serialize(serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.values.concat())
    }
}

/// RowBin serialization tuple
#[derive(Debug, Default)]
pub struct RowBinSerializeTupleVariant {
    values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeTupleVariant for RowBinSerializeTupleVariant {
    type Ok = Vec<u8>;

    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let v = value.serialize(serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.values.concat())
    }
}

/// RowBin serialization map
#[derive(Debug, Default)]
pub struct RowBinSerializeMap {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeMap for RowBinSerializeMap {
    type Ok = Vec<u8>;

    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let v = key.serialize(serializer)?;
        self.keys.push(v);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let v = value.serialize(serializer)?;
        self.values.push(v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.values.concat())
    }
}

/// RowBin serialization struct
#[derive(Debug, Default)]
pub struct RowBinSerializeStruct {
    values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeStruct for RowBinSerializeStruct {
    type Ok = Vec<u8>;

    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let serializer = RowBinSerializer::default();
        let mut key = key.serialize(serializer)?;
        let mut value = value.serialize(serializer)?;
        key.append(&mut value);
        self.values.push(key);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.values.concat())
    }
}

/// RowBin serialization struct
#[derive(Debug, Default)]
pub struct RowBinSerializeStructVariant {
    _values: Vec<Vec<u8>>,
}

impl serde::ser::SerializeStructVariant for RowBinSerializeStructVariant {
    type Ok = Vec<u8>;

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

impl Serializer for RowBinSerializer {
    type Ok = Vec<u8>;

    type Error = Error;

    type SerializeSeq = RowBinSerializeSeq;

    type SerializeTuple = RowBinSerializeTuple;

    type SerializeTupleStruct = RowBinSerializeTupleStruct;

    type SerializeTupleVariant = RowBinSerializeTupleVariant;

    type SerializeMap = RowBinSerializeMap;

    type SerializeStruct = RowBinSerializeStruct;

    type SerializeStructVariant = RowBinSerializeStructVariant;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        if v {
            Ok(vec![1])
        } else {
            Ok(vec![0])
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_le_bytes().to_vec())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string().as_bytes().to_vec())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let mut buf = vec![];
        leb128::write::unsigned(&mut buf, v.len() as u64).unwrap();
        buf.write_all(v.as_bytes()).unwrap();
        Ok(buf)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_vec())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        // NB: NULL is 1
        Ok(vec![0x01])
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        let mut buf = vec![0x00]; // 0 = NOT NULL
        let serializer = RowBinSerializer::default();
        let mut bytes = value.serialize(serializer)?;
        buf.append(&mut bytes);
        Ok(buf)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::new("unit type () is not a valid RowBin type"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::new("unit struct is not a valid RowBin type"))
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
        Ok(RowBinSerializeSeq::default())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(RowBinSerializeTuple::default())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(RowBinSerializeTupleStruct::default())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(RowBinSerializeTupleVariant::default())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(RowBinSerializeMap::default())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(RowBinSerializeStruct::default())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(RowBinSerializeStructVariant::default())
    }
}
