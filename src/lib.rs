extern crate bincode;
extern crate bytekey;
extern crate futures;
extern crate serde;
pub extern crate sled_web;

use futures::future;
use hyper::rt::{Future, Stream};
use serde::{Deserialize, Serialize};
pub use sled_web::{hyper, sled};
use std::error::Error as StdError;
use std::{fmt, ops};
use std::marker::PhantomData;
use std::result::Result as StdResult;

pub mod reversible;

/// A single table within a `sled::Tree`.
pub trait Table {
    /// The type used to distinguish tables from one another.
    type Id: Id;
    /// The type used as a key into the table.
    type Key: Key;
    /// The type used as the value associated with a key.
    type Value: Value;
    /// A constant, unique identifier that distinguishes the table from all others at runtime.
    const ID: Self::Id;
}

/// Types that may be used as a **Id** to distinguish a **Table** from others.
pub trait Id: PartialEq + for<'de> Deserialize<'de> + Serialize {}

/// Types that may be used as a **Key** into a **Table**.
pub trait Key: for<'de> Deserialize<'de> + Serialize {}

/// Types that may be used as a **Value** within a **Table**.
pub trait Value: for<'de> Deserialize<'de> + Serialize {}

/// ID types where the following ID is known.
///
/// This is necessary to discover the upper bound for the `max` method.
pub trait NextId: Id {
    /// The ID that follows self.
    fn next_id(&self) -> Self;
}

/// **Read-only** access to a **Table** via a **sled_web::Client**.
#[derive(Debug)]
pub struct Reader<T> {
    client: sled_web::Client,
    _table: PhantomData<T>,
}

/// Read and write access to a **Table** via a **sled_web::Client**.
#[derive(Debug)]
pub struct Writer<T> {
    reader: Reader<T>,
}

/// The possible errors that might occur while reading/writing a **Table** within a **sled::Tree**.
#[derive(Debug)]
pub enum Error {
    Bincode(bincode::Error),
    Bytekey(bytekey::Error),
    SledWeb(sled_web::client::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl<T> Reader<T>
where
    T: Table,
{
    /// Retrieve a value from the **Tree** if it exists.
    pub fn get(&self, key: &T::Key) -> impl Future<Item = Option<T::Value>, Error = Error> {
        let client = self.client.clone();
        write_key_future::<T>(key)
            .and_then(move |key_bytes| {
                client
                    .get(key_bytes)
                    .map_err(Error::SledWeb)
                    .and_then(|opt| match opt {
                        None => Ok(None),
                        Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
                    })
            })
    }

    /// Iterate over the byte representation of all key/value pairs within the table.
    ///
    /// The yielded bytes for each entry are laid out as follows:
    ///
    /// ```txt
    /// ([T::ID, T::Key], [T::Value])
    /// ```
    pub fn iter_bytes(&self) -> impl Stream<Item = (Vec<u8>, Vec<u8>), Error = Error> {
        let client = self.client.clone();
        write_id_future(&T::ID)
            .map(move |id_bytes| {
                let id_len = id_bytes.len();
                client
                    .scan(id_bytes.clone())
                    .map_err(Error::SledWeb)
                    .filter(move |(ref id_k, _v)| id_k[..id_len] == id_bytes[..])
            })
            .flatten_stream()
    }

    /// Iterate over all key value pairs in the table.
    pub fn iter(&self) -> impl Stream<Item = (T::Key, T::Value), Error = Error> {
        let iter_bytes = self.iter_bytes();
        write_id_future(&T::ID)
            .map(move |id_bytes| {
                let id_len = id_bytes.len();
                iter_bytes
                    .and_then(move |(id_k, v)| {
                        let k = &id_k[id_len..];
                        entry_from_bytes_future::<T>(k, &v)
                    })
            })
            .flatten_stream()
    }

    /// Iterate over the byte representation of all key/value pairs within the table.
    ///
    /// The yielded bytes for each entry are laid out as follows:
    ///
    /// ```txt
    /// ([T::ID, T::Key], [T::Value])
    /// ```
    pub fn scan_bytes(&self, k: &T::Key) -> impl Stream<Item = (Vec<u8>, Vec<u8>), Error = Error> {
        let client = self.client.clone();
        write_key_future::<T>(k)
            .join(write_id_future(&T::ID))
            .map(move |(id_bytes, key_bytes)| {
                let id_len = id_bytes.len();
                client
                    .scan(key_bytes)
                    .map_err(Error::SledWeb)
                    .filter(move |(ref id_k, _v)| id_k[..id_len] == id_bytes[..])
            })
            .flatten_stream()
    }

    /// Iterate over the byte representation of all key/value pairs within the table within the
    /// given range.
    ///
    /// The yielded bytes for each entry are laid out as follows:
    ///
    /// ```txt
    /// ([T::ID, T::Key], [T::Value])
    /// ```
    pub fn scan_range_bytes(
        &self,
        start: &T::Key,
        end: &T::Key,
    ) -> impl Stream<Item = (Vec<u8>, Vec<u8>), Error = Error> {
        let client = self.client.clone();
        let start = write_key_future::<T>(start);
        let end = write_key_future::<T>(end);
        start.join(end)
            .map(move |(start, end)| client.scan_range(start, end).map_err(Error::SledWeb))
            .flatten_stream()
    }

    /// Iterate over tuples of keys and values, starting at the provided key.
    pub fn scan(&self, key: &T::Key) -> impl Stream<Item = (T::Key, T::Value), Error = Error> {
        let scan_bytes = self.scan_bytes(key);
        write_id_future(&T::ID)
            .map(move |id_bytes| {
                let id_len = id_bytes.len();
                scan_bytes
                    .and_then(move |(id_k, v)| {
                        let k = &id_k[id_len..];
                        entry_from_bytes_future::<T>(k, &v)
                    })
            })
            .flatten_stream()
    }

    /// Iterate over tuples of keys and values, starting at the provided key.
    pub fn scan_range(
        &self,
        start: &T::Key,
        end: &T::Key,
    ) -> impl Stream<Item = (T::Key, T::Value), Error = Error> {
        let scan_bytes = self.scan_range_bytes(start, end);
        write_id_future(&T::ID)
            .map(move |id_bytes| {
                let id_len = id_bytes.len();
                scan_bytes
                    .and_then(move |(id_k, v)| {
                        let k = &id_k[id_len..];
                        entry_from_bytes_future::<T>(k, &v)
                    })
            })
            .flatten_stream()
    }

    /// Return the entry that precedes the given key.
    ///
    /// Returns `None` if no such key exists.
    pub fn pred(&self, k: &T::Key) -> impl Future<Item = Option<(T::Key, T::Value)>, Error = Error>
    where
        T::Key: PartialEq,
    {
        let client = self.client.clone();
        write_key_future::<T>(k)
            .and_then(move |key_bytes| client.pred(key_bytes).map_err(Error::SledWeb))
            .join(write_id_future(&T::ID))
            .and_then(move |(opt, id_bytes)| {
                let id_len = id_bytes.len();
                opt.and_then(move |(id_k, v)| {
                    let (id, k) = id_k.split_at(id_len);
                    if id == &id_bytes[..] {
                        Some(entry_from_bytes_future::<T>(k, &v))
                    } else {
                        None
                    }
                })
            })
    }

    /// Return the entry that matches or precedes the given key.
    ///
    /// Returns `None` if no such key exists.
    pub fn pred_incl(
        &self,
        k: &T::Key,
    ) -> impl Future<Item = Option<(T::Key, T::Value)>, Error = Error>
    where
        T::Key: PartialEq,
    {
        let client = self.client.clone();
        write_key_future::<T>(k)
            .and_then(move |key_bytes| client.pred_incl(key_bytes).map_err(Error::SledWeb))
            .join(write_id_future(&T::ID))
            .and_then(move |(opt, id_bytes)| {
                let id_len = id_bytes.len();
                opt.and_then(|(id_k, v)| {
                    let (id, k) = id_k.split_at(id_len);
                    match id == &id_bytes[..] {
                        true => Some(entry_from_bytes_future::<T>(k, &v)),
                        false => None,
                    }
                })
            })
    }

    /// Return the entry that is the successor of the given key.
    ///
    /// Returns `None` if no such key exists.
    ///
    /// This is similar to using the `scan(key).next()` method, but is non-inclusive of the given
    /// key.
    pub fn succ(&self, k: &T::Key) -> impl Future<Item = Option<(T::Key, T::Value)>, Error = Error>
    where
        T::Key: PartialEq,
    {
        let client = self.client.clone();
        write_key_future::<T>(k)
            .and_then(move |key_bytes| client.succ(key_bytes).map_err(Error::SledWeb))
            .join(write_id_future(&T::ID))
            .and_then(move |(opt, id_bytes)| {
                let id_len = id_bytes.len();
                opt.and_then(|(id_k, v)| {
                    let (id, k) = id_k.split_at(id_len);
                    match id == &id_bytes[..] {
                        true => Some(entry_from_bytes_future::<T>(k, &v)),
                        false => None,
                    }
                })
            })
    }

    /// Return the entry that is equal to or the successor of the given key.
    ///
    /// Returns `None` if no such key exists.
    ///
    /// This is similar to using the `scan(key).next()` method.
    pub fn succ_incl(
        &self,
        key: &T::Key,
    ) -> impl Future<Item = Option<(T::Key, T::Value)>, Error = Error> {
        let client = self.client.clone();
        write_key_future::<T>(key)
            .and_then(move |key_bytes| client.succ_incl(key_bytes).map_err(Error::SledWeb))
            .join(write_id_future(&T::ID))
            .and_then(move |(opt, id_bytes)| {
                let id_len = id_bytes.len();
                opt.and_then(|(id_k, v)| {
                    let (id, k) = id_k.split_at(id_len);
                    match id == &id_bytes[..] {
                        true => Some(entry_from_bytes_future::<T>(k, &v)),
                        false => None,
                    }
                })
            })
    }

    /// Return the minimum entry within the table.
    ///
    /// This is similar to using the `iter().next()` method.
    pub fn min(&self) -> impl Future<Item = Option<(T::Key, T::Value)>, Error = Error> {
        let client = self.client.clone();
        write_id_future(&T::ID)
            .and_then(move |id_bytes| {
                client
                    .succ_incl(id_bytes.clone())
                    .map_err(Error::SledWeb)
                    .map(move |opt| (id_bytes, opt))
            })
            .and_then(move |(id_bytes, opt)| {
                opt.and_then(move |(id_k, v)| {
                    let (id, k) = id_k.split_at(id_bytes.len());
                    match id == &id_bytes[..] {
                        true => Some(entry_from_bytes_future::<T>(k, &v)),
                        false => None,
                    }
                })
            })
    }

    /// The size of the table on disk in bytes.
    ///
    /// TODO: This implementation currently requires downloading every entry from the table. This
    /// method should be moved upstream to the `sled_web` crate.
    pub fn size_bytes(&self) -> impl Future<Item = usize, Error = Error> {
        self.iter_bytes()
            .fold(0, |acc, (k, v)| future::ok::<_, Error>(acc + k.len() + v.len()))
    }
}

impl<T> Reader<T>
where
    T: Table,
    T::Id: NextId,
{
    /// Return the greatest entry that exists within the table.
    pub fn max(&self) -> impl Future<Item = Option<(T::Key, T::Value)>, Error = Error>
    where
        T::Key: PartialEq,
    {
        let client = self.client.clone();
        let next_table_id = T::ID.next_id();
        write_id_future(&next_table_id)
            .and_then(move |next_id_bytes| client.pred(next_id_bytes).map_err(Error::SledWeb))
            .join(write_id_future(&T::ID))
            .and_then(|(opt, id_bytes)| {
                let id_len = id_bytes.len();
                opt.and_then(|(id_k, v)| {
                    let (id, k) = id_k.split_at(id_len);
                    match id[..] == id_bytes[..] {
                        true => Some(entry_from_bytes_future::<T>(k, &v)),
                        false => None,
                    }
                })
            })
    }
}

impl<T> Writer<T>
where
    T: Table,
{
    /// Set the given **key** to a new **value**.
    pub fn set(&self, key: &T::Key, value: &T::Value) -> impl Future<Item = (), Error = Error> {
        let client = self.client.clone();
        write_key_future::<T>(key)
            .join(future::result(bincode::serialize(value).map_err(From::from)))
            .and_then(move |(k, v)| client.set(k, v).map_err(From::from))
    }

    /// Remove a value from the **Tree** if it exists.
    pub fn del(&self, key: &T::Key) -> impl Future<Item = Option<T::Value>, Error = Error> {
        let client= self.client.clone();
        write_key_future::<T>(key)
            .and_then(move |k| client.del(k).map_err(From::from))
            .and_then(|opt| {
                opt.map(|v| future::result(bincode::deserialize(&v).map_err(From::from)))
            })
    }

    /// Compare and swap. Capable of unique creation, conditional modification, or deletion.
    ///
    /// If old is None, this will only set the value if it doesn't exist yet. If new is None, will
    /// delete the value if old is correct. If both old and new are Some, will modify the value if
    /// old is correct.
    ///
    /// If `Tree` is read-only, will do nothing.
    pub fn cas(
        &self,
        key: &T::Key,
        old: Option<&T::Value>,
        new: Option<&T::Value>,
    ) -> impl Future<Item = StdResult<(), Option<T::Value>>, Error = Error> {
        let client = self.client.clone();
        let key = write_key_future::<T>(key);
        let write_opt_value_future = |v: Option<&T::Value>| {
            match v {
                None => future::ok(None),
                Some(v) => future::result(bincode::serialize(v).map(Some).map_err(From::from)),
            }
        };
        let old = write_opt_value_future(old);
        let new = write_opt_value_future(new);
        key.join3(old, new)
            .and_then(move |(k, o, n)| client.cas(k, o, n).map_err(From::from))
            .and_then(|res| {
                match res {
                    Ok(()) => future::ok(Ok(())),
                    Err(None) => future::ok(Err(None)),
                    Err(Some(b)) => match bincode::deserialize(&b) {
                        Ok(v) => future::ok(Err(Some(v))),
                        Err(err) => future::err(From::from(err)),
                    }
                }
            })
    }

    /// Merge `value` into the entry at the given key.
    pub fn merge(&self, key: &T::Key, value: &T::Value) -> impl Future<Item = (), Error = Error> {
        let client = self.client.clone();
        write_key_future::<T>(key)
            .join(future::result(bincode::serialize(value).map_err(From::from)))
            .and_then(move |(k, v)| client.merge(k, v).map_err(From::from))
    }
}

impl<T> Id for T where T: PartialEq + for<'de> Deserialize<'de> + Serialize {}

impl<T> Key for T where T: for<'de> Deserialize<'de> + Serialize {}

impl<T> Value for T where T: for<'de> Deserialize<'de> + Serialize {}

impl<T> From<sled_web::Client> for Reader<T> {
    fn from(client: sled_web::Client) -> Self {
        let _table = PhantomData;
        Reader { client, _table }
    }
}

impl<T> From<sled_web::Client> for Writer<T> {
    fn from(client: sled_web::Client) -> Self {
        let reader = client.into();
        Writer { reader }
    }
}

impl<T> From<Writer<T>> for Reader<T> {
    fn from(writer: Writer<T>) -> Self {
        writer.reader
    }
}

impl<T> Clone for Reader<T> {
    fn clone(&self) -> Self {
        let client = self.client.clone();
        let _table = PhantomData;
        Reader { client, _table }
    }
}

impl<T> Clone for Writer<T> {
    fn clone(&self) -> Self {
        let reader = self.reader.clone();
        Writer { reader }
    }
}

impl<T> ops::Deref for Writer<T> {
    type Target = Reader<T>;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::SledWeb(ref err) => err.description(),
            Error::Bincode(ref err) => err.description(),
            Error::Bytekey(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::SledWeb(ref err) => Some(err),
            Error::Bincode(ref err) => Some(err),
            Error::Bytekey(ref err) => Some(err),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

impl From<sled_web::client::Error> for Error {
    fn from(e: sled_web::client::Error) -> Self {
        Error::SledWeb(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Bincode(e)
    }
}

impl From<bytekey::Error> for Error {
    fn from(e: bytekey::Error) -> Self {
        Error::Bytekey(e)
    }
}

impl From<bytekey::ser::Error> for Error {
    fn from(e: bytekey::ser::Error) -> Self {
        Error::Bytekey(e.into())
    }
}

impl From<bytekey::de::Error> for Error {
    fn from(e: bytekey::de::Error) -> Self {
        Error::Bytekey(e.into())
    }
}

/// A function to simplify the implementation of a `sled::Tree` merge operator for a single table.
///
/// **Panics** if serialization or deserialization of keys or values fail.
pub fn merge<T, F>(id_key: &[u8], old: Option<&[u8]>, new: &[u8], f: F) -> Vec<u8>
where
    T: Table,
    F: Fn(T::Key, Option<T::Value>, T::Value) -> T::Value,
{
    let id_bytes = write_id(&T::ID).expect("failed to write table ID");
    let (_id, key) = id_key.split_at(id_bytes.len());
    let key = bytekey::deserialize(key).expect("failed to deserialize key");
    let old = old.map(|old| bincode::deserialize(old).expect("failed to deserialize value"));
    let new = bincode::deserialize(new).expect("failed to deserialize value");
    let merged = f(key, old, new);
    bincode::serialize(&merged).expect("failed to deserialize merged value")
}

/// Write a key for table `T` to bytes.
///
/// This simply pre-pends the serialized `key` with a serialised instance of the table `ID`.
pub fn write_key<T>(key: &T::Key) -> bytekey::Result<Vec<u8>>
where
    T: Table,
{
    let mut key_bytes = vec![];
    bytekey::serialize_into(&mut key_bytes, &T::ID)?;
    bytekey::serialize_into(&mut key_bytes, key)?;
    Ok(key_bytes)
}

/// The same as `write_key` but produces a future.
pub fn write_key_future<T>(key: &T::Key) -> impl Future<Item = Vec<u8>, Error = Error>
where
    T: Table,
{
    future::result(write_key::<T>(key)).map_err(From::from)
}

/// Deserialize the byte representation of an entry into its table types.
///
/// Assumes that the key *only* contains bytes of the serialized `T::Key` and is not prepended with
/// the `T::ID`..
pub fn entry_from_bytes<T>(k: &[u8], v: &[u8]) -> Result<(T::Key, T::Value)>
where
    T: Table,
{
    let key = bytekey::deserialize(k)?;
    let value = bincode::deserialize(v)?;
    Ok((key, value))
}

/// The same as `entry_from_bytes` but produces the result within a future.
pub fn entry_from_bytes_future<T>(
    k: &[u8],
    v: &[u8],
) -> impl Future<Item = (T::Key, T::Value), Error = Error>
where
    T: Table,
{
    future::result(entry_from_bytes::<T>(k, v))
}


/// Serialize the given table ID to its byte representation.
pub fn write_id<I>(id: &I) -> Result<Vec<u8>>
where
    I: Serialize,
{
    bytekey::serialize(id).map_err(From::from)
}

/// Serialize the given table ID to its byte representation, returning the result via a future.
pub fn write_id_future<I>(id: &I) -> impl Future<Item = Vec<u8>, Error = Error>
where
    I: Serialize,
{
    future::result(write_id(id))
}

/// Given a slice of bytes representing a table ID and key, read the ID and return the remaining
/// key bytes.
pub fn read_id_from_front<I>(id_key_bytes: &[u8]) -> bytekey::de::Result<(I, &[u8])>
where
    I: Id,
{
    let mut cursor = std::io::Cursor::new(id_key_bytes);
    let id = bytekey::deserialize_from(&mut cursor)?;
    let key_bytes = &id_key_bytes[cursor.position() as usize..];
    Ok((id, key_bytes))
}
