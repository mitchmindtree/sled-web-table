use {Error, Table};
use hyper::rt::Future;
use sled_web;
use std::ops;

/// An extension to the **Table** trait that allows for bi-directional conversions with some other
/// table.
pub trait Reversible: Table {
    /// The table used to perform the reverse conversion of this table.
    type ReverseTable: Table<Id = Self::Id, Key = Self::Value, Value = Self::Key>;
}

/// Read and write access to a reversible table within a `sled::Tree`.
#[derive(Debug)]
pub struct Writer<T>
where
    T: Reversible,
{
    pub(crate) table: ::Writer<T>,
    pub(crate) reverse_table: ::Writer<T::ReverseTable>,
}

/// Read-only access to a reversible table within a `sled::Tree`.
#[derive(Debug)]
pub struct Reader<T>
where
    T: Reversible,
{
    table: ::Reader<T>,
    reverse_table: ::Reader<T::ReverseTable>,
}

// Inherent implementations.

impl<T> Writer<T>
where
    T: Reversible,
{
    /// Set the given **key** to the new **value**.
    ///
    /// Also ensures that the inverse entry is added to **T::ReverseTable**.
    ///
    /// If either the key XOR value already exist, this method will `panic!` to ensure uniqueness
    /// between pairs.
    pub fn set(&self, key: &T::Key, value: &T::Value) -> impl Future<Item = (), Error = Error> {
        let fwd = self.table.set(key, value);
        let bwd = self.reverse_table.set(value, key);
        fwd.and_then(|_| bwd)
    }

    /// Remove the entry for the given **key** from the table.
    ///
    /// Also removes the reverse entry from the reverse table.
    pub fn del(&self, key: &T::Key) -> impl Future<Item = Option<T::Value>, Error = Error> {
        let rev = self.reverse_table.clone();
        self.table.del(key).and_then(move |opt| opt.map(|v| rev.del(&v).map(|_| v)))
    }

    /// Return the inverse of this table.
    pub fn inv(&self) -> Writer<T::ReverseTable>
    where
        T::ReverseTable: Reversible<ReverseTable = T>,
    {
        let reverse_table = self.table.clone();
        let table = self.reverse_table.clone();
        Writer { table, reverse_table }
    }
}

impl<T> Reader<T>
where
    T: Reversible,
    T::ReverseTable: Reversible<ReverseTable = T>,
{
    /// Read-only access to the inverse of this table, using `Value` as `Key` and vice versa.
    pub fn inv(&self) -> Reader<T::ReverseTable> {
        let reverse_table = self.table.clone();
        let table = self.reverse_table.clone();
        Reader { table, reverse_table }
    }
}

// Trait implementations.

impl<T> From<sled_web::Client> for Reader<T>
where
    T: Reversible,
{
    fn from(client: sled_web::Client) -> Self {
        let table = client.clone().into();
        let reverse_table = client.into();
        Reader {
            table,
            reverse_table,
        }
    }
}

impl<T> From<sled_web::Client> for Writer<T>
where
    T: Reversible,
{
    fn from(client: sled_web::Client) -> Self {
        let table = client.clone().into();
        let reverse_table = client.into();
        Writer {
            table,
            reverse_table,
        }
    }
}

impl<T> From<Writer<T>> for Reader<T>
where
    T: Reversible,
{
    fn from(w: Writer<T>) -> Self {
        let table = w.table.clone().into();
        let reverse_table = w.reverse_table.clone().into();
        Reader { table, reverse_table }
    }
}

impl<T> Clone for Reader<T>
where
    T: Reversible,
{
    fn clone(&self) -> Self {
        let table = self.table.clone();
        let reverse_table = self.reverse_table.clone();
        Reader { table, reverse_table }
    }
}

impl<T> Clone for Writer<T>
where
    T: Reversible,
{
    fn clone(&self) -> Self {
        let table = self.table.clone();
        let reverse_table = self.reverse_table.clone();
        Writer { table, reverse_table }
    }
}

impl<T> ops::Deref for Reader<T>
where
    T: Reversible,
{
    type Target = ::Reader<T>;
    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<T> ops::Deref for Writer<T>
where
    T: Reversible,
{
    type Target = ::Reader<T>;
    fn deref(&self) -> &Self::Target {
        &self.table
    }
}
