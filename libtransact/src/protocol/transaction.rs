/*
 * Copyright 2018 Bitwise IO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

//! The fundamental transaction.
//!
//! A transaction is a signed, opaque payload that acts as a fundamental operation inducing a state
//! change via a smart contract engine.  They are executed as part of a batch.

use hex;
use protobuf::Message;
use sha2::{Digest, Sha512};
use std;
use std::error::Error as StdError;

use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::protos;
use crate::protos::{
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};
use crate::signing;

static DEFAULT_NONCE_SIZE: usize = 32;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum HashMethod {
    SHA512,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TransactionHeader {
    batcher_public_key: Vec<u8>,
    dependencies: Vec<Vec<u8>>,
    family_name: String,
    family_version: String,
    inputs: Vec<Vec<u8>>,
    outputs: Vec<Vec<u8>>,
    nonce: Vec<u8>,
    payload_hash: Vec<u8>,
    payload_hash_method: HashMethod,
    signer_public_key: Vec<u8>,
}

impl TransactionHeader {
    pub fn batcher_public_key(&self) -> &[u8] {
        &self.batcher_public_key
    }

    pub fn dependencies(&self) -> &[Vec<u8>] {
        &self.dependencies
    }

    pub fn family_name(&self) -> &str {
        &self.family_name
    }

    pub fn family_version(&self) -> &str {
        &self.family_version
    }

    pub fn inputs(&self) -> &[Vec<u8>] {
        &self.inputs
    }

    pub fn nonce(&self) -> &[u8] {
        &self.nonce
    }

    pub fn outputs(&self) -> &[Vec<u8>] {
        &self.outputs
    }

    pub fn payload_hash(&self) -> &[u8] {
        &self.payload_hash
    }

    pub fn payload_hash_method(&self) -> &HashMethod {
        &self.payload_hash_method
    }

    pub fn signer_public_key(&self) -> &[u8] {
        &self.signer_public_key
    }
}

impl From<hex::FromHexError> for ProtoConversionError {
    fn from(e: hex::FromHexError) -> Self {
        ProtoConversionError::SerializationError(format!("{}", e))
    }
}

impl From<std::string::FromUtf8Error> for ProtoConversionError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        ProtoConversionError::SerializationError(format!("{}", e))
    }
}

impl FromProto<protos::transaction::TransactionHeader> for TransactionHeader {
    fn from_proto(
        header: protos::transaction::TransactionHeader,
    ) -> Result<Self, ProtoConversionError> {
        Ok(TransactionHeader {
            family_name: header.get_family_name().to_string(),
            family_version: header.get_family_version().to_string(),
            batcher_public_key: hex::decode(header.get_batcher_public_key())?,
            dependencies: header
                .get_dependencies()
                .iter()
                .map(|d| hex::decode(d).map_err(ProtoConversionError::from))
                .collect::<Result<_, _>>()?,
            inputs: header
                .get_inputs()
                .iter()
                .map(|d| hex::decode(d).map_err(ProtoConversionError::from))
                .collect::<Result<_, _>>()?,
            nonce: header.get_nonce().to_string().into_bytes(),
            outputs: header
                .get_outputs()
                .iter()
                .map(|d| hex::decode(d).map_err(ProtoConversionError::from))
                .collect::<Result<_, _>>()?,
            payload_hash: hex::decode(header.get_payload_sha512())?,
            payload_hash_method: HashMethod::SHA512,
            signer_public_key: hex::decode(header.get_signer_public_key())?,
        })
    }
}

impl FromNative<TransactionHeader> for protos::transaction::TransactionHeader {
    fn from_native(header: TransactionHeader) -> Result<Self, ProtoConversionError> {
        let mut proto_header = protos::transaction::TransactionHeader::new();
        proto_header.set_family_name(header.family_name().to_string());
        proto_header.set_family_version(header.family_version().to_string());
        proto_header.set_batcher_public_key(hex::encode(header.batcher_public_key()));
        proto_header.set_dependencies(header.dependencies().iter().map(hex::encode).collect());
        proto_header.set_inputs(header.inputs().iter().map(hex::encode).collect());
        proto_header.set_nonce(String::from_utf8(header.nonce().to_vec())?);
        proto_header.set_outputs(header.outputs().iter().map(hex::encode).collect());
        proto_header.set_payload_sha512(hex::encode(header.payload_hash()));
        proto_header.set_signer_public_key(hex::encode(header.signer_public_key()));
        Ok(proto_header)
    }
}

impl FromBytes<TransactionHeader> for TransactionHeader {
    fn from_bytes(bytes: &[u8]) -> Result<TransactionHeader, ProtoConversionError> {
        let proto: protos::transaction::TransactionHeader = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get TransactionHeader from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for TransactionHeader {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from TransactionHeader".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::transaction::TransactionHeader> for TransactionHeader {}
impl IntoNative<TransactionHeader> for protos::transaction::TransactionHeader {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Transaction {
    header: Vec<u8>,
    header_signature: String,
    payload: Vec<u8>,
}

impl Transaction {
    pub fn new(header: Vec<u8>, header_signature: String, payload: Vec<u8>) -> Self {
        Transaction {
            header,
            header_signature,
            payload,
        }
    }

    pub fn header(&self) -> &[u8] {
        &self.header
    }

    pub fn header_signature(&self) -> &str {
        &self.header_signature
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_pair(self) -> Result<TransactionPair, TransactionBuildError> {
        let header = TransactionHeader::from_bytes(&self.header)?;

        Ok(TransactionPair {
            transaction: self,
            header,
        })
    }
}

impl From<protos::transaction::Transaction> for Transaction {
    fn from(transaction: protos::transaction::Transaction) -> Self {
        Transaction {
            header: transaction.get_header().to_vec(),
            header_signature: transaction.get_header_signature().to_string(),
            payload: transaction.get_payload().to_vec(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TransactionPair {
    transaction: Transaction,
    header: TransactionHeader,
}

impl TransactionPair {
    pub fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn header(&self) -> &TransactionHeader {
        &self.header
    }

    pub fn take(self) -> (Transaction, TransactionHeader) {
        (self.transaction, self.header)
    }
}

#[derive(Debug)]
pub enum TransactionBuildError {
    DeserializationError(String),
    MissingField(String),
    SerializationError(String),
    SigningError(String),
}

impl StdError for TransactionBuildError {
    fn description(&self) -> &str {
        match *self {
            TransactionBuildError::DeserializationError(ref msg) => msg,
            TransactionBuildError::MissingField(ref msg) => msg,
            TransactionBuildError::SerializationError(ref msg) => msg,
            TransactionBuildError::SigningError(ref msg) => msg,
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            TransactionBuildError::DeserializationError(_) => None,
            TransactionBuildError::MissingField(_) => None,
            TransactionBuildError::SerializationError(_) => None,
            TransactionBuildError::SigningError(_) => None,
        }
    }
}

impl std::fmt::Display for TransactionBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            TransactionBuildError::DeserializationError(ref s) => {
                write!(f, "DeserializationError: {}", s)
            }
            TransactionBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
            TransactionBuildError::SerializationError(ref s) => {
                write!(f, "SerializationError: {}", s)
            }
            TransactionBuildError::SigningError(ref s) => write!(f, "SigningError: {}", s),
        }
    }
}

impl From<ProtoConversionError> for TransactionBuildError {
    fn from(e: ProtoConversionError) -> Self {
        TransactionBuildError::DeserializationError(format!("{}", e))
    }
}

#[derive(Default, Clone)]
pub struct TransactionBuilder {
    batcher_public_key: Option<Vec<u8>>,
    dependencies: Option<Vec<Vec<u8>>>,
    family_name: Option<String>,
    family_version: Option<String>,
    inputs: Option<Vec<Vec<u8>>>,
    outputs: Option<Vec<Vec<u8>>>,
    nonce: Option<Vec<u8>>,
    payload_hash_method: Option<HashMethod>,
    payload: Option<Vec<u8>>,
}

impl TransactionBuilder {
    pub fn new() -> Self {
        TransactionBuilder::default()
    }

    pub fn with_batcher_public_key(mut self, batcher_public_key: Vec<u8>) -> TransactionBuilder {
        self.batcher_public_key = Some(batcher_public_key);
        self
    }

    pub fn with_dependencies(mut self, dependencies: Vec<Vec<u8>>) -> TransactionBuilder {
        self.dependencies = Some(dependencies);
        self
    }

    pub fn with_family_name(mut self, family_name: String) -> TransactionBuilder {
        self.family_name = Some(family_name);
        self
    }

    pub fn with_family_version(mut self, family_version: String) -> TransactionBuilder {
        self.family_version = Some(family_version);
        self
    }

    pub fn with_inputs(mut self, inputs: Vec<Vec<u8>>) -> TransactionBuilder {
        self.inputs = Some(inputs);
        self
    }

    pub fn with_outputs(mut self, outputs: Vec<Vec<u8>>) -> TransactionBuilder {
        self.outputs = Some(outputs);
        self
    }

    pub fn with_nonce(mut self, nonce: Vec<u8>) -> TransactionBuilder {
        self.nonce = Some(nonce);
        self
    }

    pub fn with_payload_hash_method(
        mut self,
        payload_hash_method: HashMethod,
    ) -> TransactionBuilder {
        self.payload_hash_method = Some(payload_hash_method);
        self
    }

    pub fn with_payload(mut self, payload: Vec<u8>) -> TransactionBuilder {
        self.payload = Some(payload);
        self
    }

    pub fn build_pair(
        self,
        signer: &signing::Signer,
    ) -> Result<TransactionPair, TransactionBuildError> {
        let batcher_public_key = self
            .batcher_public_key
            .unwrap_or_else(|| signer.public_key().to_vec());
        let dependencies = self.dependencies.unwrap_or_else(|| vec![]);
        let family_name = self.family_name.ok_or_else(|| {
            TransactionBuildError::MissingField("'family_name' field is required".to_string())
        })?;
        let family_version = self.family_version.ok_or_else(|| {
            TransactionBuildError::MissingField("'family_version' field is required".to_string())
        })?;
        let inputs = self.inputs.ok_or_else(|| {
            TransactionBuildError::MissingField("'inputs' field is required".to_string())
        })?;
        let outputs = self.outputs.ok_or_else(|| {
            TransactionBuildError::MissingField("'outputs' field is required".to_string())
        })?;
        let nonce = self.nonce.unwrap_or_else(|| {
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(DEFAULT_NONCE_SIZE)
                .collect::<String>()
                .as_bytes()
                .to_vec()
        });
        let payload_hash_method = self.payload_hash_method.ok_or_else(|| {
            TransactionBuildError::MissingField(
                "'payload_hash_method' field is required".to_string(),
            )
        })?;
        let payload = self.payload.ok_or_else(|| {
            TransactionBuildError::MissingField("'payload' field is required".to_string())
        })?;
        let signer_public_key = signer.public_key().to_vec();

        let payload_hash = match payload_hash_method {
            HashMethod::SHA512 => {
                let mut hasher = Sha512::new();
                hasher.input(&payload);
                hasher.result().to_vec()
            }
        };

        let header = TransactionHeader {
            batcher_public_key,
            dependencies,
            family_name,
            family_version,
            inputs,
            outputs,
            nonce,
            payload_hash,
            payload_hash_method,
            signer_public_key,
        };

        let header_proto: protos::transaction::TransactionHeader = header
            .clone()
            .into_proto()
            .map_err(|e| TransactionBuildError::SerializationError(format!("{}", e)))?;
        let header_bytes = header_proto
            .write_to_bytes()
            .map_err(|e| TransactionBuildError::SerializationError(format!("{}", e)))?;

        let header_signature = hex::encode(
            signer
                .sign(&header_bytes)
                .map_err(|e| TransactionBuildError::SigningError(format!("{}", e)))?,
        );

        let transaction = Transaction {
            header: header_bytes,
            header_signature,
            payload,
        };

        Ok(TransactionPair {
            transaction,
            header,
        })
    }

    pub fn build(self, signer: &signing::Signer) -> Result<Transaction, TransactionBuildError> {
        Ok(self.build_pair(signer)?.transaction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "sawtooth-compat")]
    use crate::protos;
    use crate::signing::hash::HashSigner;
    use crate::signing::Signer;

    #[cfg(feature = "sawtooth-compat")]
    use protobuf::Message;

    #[cfg(feature = "sawtooth-compat")]
    use sawtooth_sdk;

    static FAMILY_NAME: &str = "test_family";
    static FAMILY_VERSION: &str = "0.1";
    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";
    static KEY5: &str = "555555555555555555555555555555555555555555555555555555555555555555";
    static KEY6: &str = "666666666666666666666666666666666666666666666666666666666666666666";
    static KEY7: &str = "777777777777777777777777777777777777777777777777777777777777777777";
    static KEY8: &str = "888888888888888888888888888888888888888888888888888888888888888888";
    static NONCE: &str = "f9kdzz";
    static HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static SIGNATURE1: &str =
        "sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1";

    fn check_builder_transaction(signer: &Signer, pair: &TransactionPair) {
        let payload_hash = match pair.header().payload_hash_method() {
            HashMethod::SHA512 => {
                let mut hasher = Sha512::new();
                hasher.input(&pair.transaction().payload());
                hasher.result().to_vec()
            }
        };

        assert_eq!(KEY1, hex::encode(pair.header().batcher_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            pair.header().dependencies()
        );
        assert_eq!(FAMILY_NAME, pair.header.family_name());
        assert_eq!(FAMILY_VERSION, pair.header.family_version());
        assert_eq!(
            vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            pair.header().inputs()
        );
        assert_eq!(
            vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            pair.header().outputs()
        );
        assert_eq!(payload_hash, pair.header().payload_hash());
        assert_eq!(HashMethod::SHA512, *pair.header().payload_hash_method());
        assert_eq!(signer.public_key(), pair.header().signer_public_key());
    }

    #[test]
    fn transaction_builder_chain() {
        let signer = HashSigner::new();

        let pair = TransactionBuilder::new()
            .with_batcher_public_key(hex::decode(KEY1).unwrap())
            .with_dependencies(vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()])
            .with_family_name(FAMILY_NAME.to_string())
            .with_family_version(FAMILY_VERSION.to_string())
            .with_inputs(vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ])
            .with_nonce(NONCE.to_string().into_bytes())
            .with_outputs(vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ])
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(BYTES2.to_vec())
            .build_pair(&signer)
            .unwrap();

        check_builder_transaction(&signer, &pair);
    }

    #[test]
    fn transaction_builder_seperate() {
        let signer = HashSigner::new();

        let mut builder = TransactionBuilder::new();
        builder = builder.with_batcher_public_key(hex::decode(KEY1).unwrap());
        builder =
            builder.with_dependencies(vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()]);
        builder = builder.with_family_name(FAMILY_NAME.to_string());
        builder = builder.with_family_version(FAMILY_VERSION.to_string());
        builder = builder.with_inputs(vec![
            hex::decode(KEY4).unwrap(),
            hex::decode(&KEY5[0..4]).unwrap(),
        ]);
        builder = builder.with_nonce(NONCE.to_string().into_bytes());
        builder = builder.with_outputs(vec![
            hex::decode(KEY6).unwrap(),
            hex::decode(&KEY7[0..4]).unwrap(),
        ]);
        builder = builder.with_payload_hash_method(HashMethod::SHA512);
        builder = builder.with_payload(BYTES2.to_vec());
        let pair = builder.build_pair(&signer).unwrap();

        check_builder_transaction(&signer, &pair);
    }

    #[test]
    fn transaction_header_fields() {
        let header = TransactionHeader {
            batcher_public_key: hex::decode(KEY1).unwrap(),
            dependencies: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
            family_name: FAMILY_NAME.to_string(),
            family_version: FAMILY_VERSION.to_string(),
            inputs: vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            nonce: NONCE.to_string().into_bytes(),
            outputs: vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            payload_hash: hex::decode(HASH).unwrap(),
            payload_hash_method: HashMethod::SHA512,
            signer_public_key: hex::decode(KEY8).unwrap(),
        };
        assert_eq!(KEY1, hex::encode(header.batcher_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            header.dependencies()
        );
        assert_eq!(FAMILY_NAME, header.family_name());
        assert_eq!(FAMILY_VERSION, header.family_version());
        assert_eq!(
            vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            header.inputs()
        );
        assert_eq!(
            vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            header.outputs()
        );
        assert_eq!(HASH, hex::encode(header.payload_hash()));
        assert_eq!(HashMethod::SHA512, *header.payload_hash_method());
        assert_eq!(KEY8, hex::encode(header.signer_public_key()));
    }

    #[test]
    // test that the transaction header can be converted into bytes and back correctly
    fn transaction_header_bytes() {
        let original = TransactionHeader {
            batcher_public_key: hex::decode(KEY1).unwrap(),
            dependencies: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
            family_name: FAMILY_NAME.to_string(),
            family_version: FAMILY_VERSION.to_string(),
            inputs: vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            nonce: NONCE.to_string().into_bytes(),
            outputs: vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            payload_hash: hex::decode(HASH).unwrap(),
            payload_hash_method: HashMethod::SHA512,
            signer_public_key: hex::decode(KEY8).unwrap(),
        };

        let header_bytes = original.clone().into_bytes().unwrap();
        let header = TransactionHeader::from_bytes(&header_bytes).unwrap();

        assert_eq!(
            hex::encode(original.batcher_public_key()),
            hex::encode(header.batcher_public_key())
        );
        assert_eq!(original.dependencies(), header.dependencies());
        assert_eq!(original.family_name(), header.family_name());
        assert_eq!(original.family_version(), header.family_version());
        assert_eq!(original.inputs(), header.inputs());
        assert_eq!(original.outputs(), header.outputs());
        assert_eq!(
            hex::encode(original.payload_hash()),
            hex::encode(header.payload_hash())
        );
        assert_eq!(
            *original.payload_hash_method(),
            *header.payload_hash_method()
        );
        assert_eq!(
            hex::encode(original.signer_public_key()),
            hex::encode(header.signer_public_key())
        );
    }

    #[cfg(feature = "sawtooth-compat")]
    #[test]
    fn transaction_header_sawtooth10_compatibility() {
        // Create protobuf bytes using the Sawtooth SDK
        let mut proto = sawtooth_sdk::messages::transaction::TransactionHeader::new();
        proto.set_batcher_public_key(KEY1.to_string());
        proto.set_dependencies(protobuf::RepeatedField::from_vec(vec![
            KEY2.to_string(),
            KEY3.to_string(),
        ]));
        proto.set_family_name(FAMILY_NAME.to_string());
        proto.set_family_version(FAMILY_VERSION.to_string());
        proto.set_inputs(protobuf::RepeatedField::from_vec(vec![
            KEY4.to_string(),
            (&KEY5[0..4]).to_string(),
        ]));
        proto.set_nonce(NONCE.to_string());
        proto.set_outputs(protobuf::RepeatedField::from_vec(vec![
            KEY6.to_string(),
            (&KEY7[0..4]).to_string(),
        ]));
        proto.set_payload_sha512(HASH.to_string());
        proto.set_signer_public_key(KEY8.to_string());
        let header_bytes = proto.write_to_bytes().unwrap();

        // Deserialize the header bytes into our protobuf
        let header_proto: protos::transaction::TransactionHeader =
            protobuf::parse_from_bytes(&header_bytes).unwrap();

        // Convert to a TransactionHeader
        let header: TransactionHeader = header_proto.into_native().unwrap();

        assert_eq!(KEY1, hex::encode(header.batcher_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
            header.dependencies()
        );
        assert_eq!(FAMILY_NAME, header.family_name());
        assert_eq!(FAMILY_VERSION, header.family_version());
        assert_eq!(
            vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap()
            ],
            header.inputs()
        );
        assert_eq!(NONCE, String::from_utf8(header.nonce().to_vec()).unwrap());
        assert_eq!(
            vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap()
            ],
            header.outputs()
        );
        assert_eq!(hex::decode(HASH).unwrap(), header.payload_hash());
        assert_eq!(HashMethod::SHA512, *header.payload_hash_method());
        assert_eq!(hex::decode(KEY8).unwrap(), header.signer_public_key());
    }

    #[test]
    fn transaction_fields() {
        let transaction = Transaction {
            header: BYTES1.to_vec(),
            header_signature: SIGNATURE1.to_string(),
            payload: BYTES2.to_vec(),
        };

        assert_eq!(BYTES1.to_vec(), transaction.header());
        assert_eq!(SIGNATURE1, transaction.header_signature());
        assert_eq!(BYTES2.to_vec(), transaction.payload());
    }

    #[cfg(feature = "sawtooth-compat")]
    #[test]
    fn transaction_sawtooth10_compatibility() {
        // Create protobuf bytes using the Sawtooth SDK
        let mut proto = sawtooth_sdk::messages::transaction::Transaction::new();
        proto.set_header(BYTES1.to_vec());
        proto.set_header_signature(SIGNATURE1.to_string());
        proto.set_payload(BYTES2.to_vec());
        let transaction_bytes = proto.write_to_bytes().unwrap();

        // Deserialize the header bytes into our protobuf
        let transaction_proto: protos::transaction::Transaction =
            protobuf::parse_from_bytes(&transaction_bytes).unwrap();

        // Convert to a Transaction
        let transaction: Transaction = transaction_proto.into();

        assert_eq!(BYTES1.to_vec(), transaction.header());
        assert_eq!(SIGNATURE1, transaction.header_signature());
        assert_eq!(BYTES2.to_vec(), transaction.payload());
    }
}

#[cfg(all(feature = "nightly", test))]
mod benchmarks {
    extern crate test;
    use super::*;
    use test::Bencher;

    use crate::protos;
    use crate::signing::hash::HashSigner;

    static FAMILY_NAME: &str = "test_family";
    static FAMILY_VERSION: &str = "0.1";
    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";
    static KEY5: &str = "555555555555555555555555555555555555555555555555555555555555555555";
    static KEY6: &str = "666666666666666666666666666666666666666666666666666666666666666666";
    static KEY7: &str = "777777777777777777777777777777777777777777777777777777777777777777";
    static KEY8: &str = "888888888888888888888888888888888888888888888888888888888888888888";
    static NONCE: &str = "f9kdzz";
    static HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static SIGNATURE1: &str =
        "sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1";

    #[bench]
    fn bench_transaction_builder(b: &mut Bencher) {
        let signer = HashSigner::new();
        let transaction = TransactionBuilder::new()
            .with_batcher_public_key(hex::decode(KEY1).unwrap())
            .with_dependencies(vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()])
            .with_family_name(FAMILY_NAME.to_string())
            .with_family_version(FAMILY_VERSION.to_string())
            .with_inputs(vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ])
            .with_nonce(NONCE.to_string().into_bytes())
            .with_outputs(vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ])
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(BYTES2.to_vec());

        b.iter(|| transaction.clone().build_pair(&signer));
    }

    #[bench]
    fn bench_transaction_creation(b: &mut Bencher) {
        b.iter(|| Transaction {
            header: BYTES1.to_vec(),
            header_signature: SIGNATURE1.to_string(),
            payload: BYTES2.to_vec(),
        });
    }

    #[bench]
    fn bench_txn_header_into_proto(b: &mut Bencher) {
        let header = TransactionHeader {
            batcher_public_key: hex::decode(KEY1).unwrap(),
            dependencies: vec![hex::decode(KEY2).unwrap()],
            family_name: FAMILY_NAME.to_string(),
            family_version: FAMILY_VERSION.to_string(),
            inputs: vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            nonce: NONCE.to_string().into_bytes(),
            outputs: vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            payload_hash: hex::decode(HASH).unwrap(),
            payload_hash_method: HashMethod::SHA512,
            signer_public_key: hex::decode(KEY8).unwrap(),
        };

        b.iter(|| header.clone().into_proto());
    }

    #[bench]
    fn bench_txn_header_into_native(b: &mut Bencher) {
        let mut proto = protos::transaction::TransactionHeader::new();
        proto.set_batcher_public_key(KEY1.to_string());
        proto.set_dependencies(protobuf::RepeatedField::from_vec(vec![
            KEY2.to_string(),
            KEY3.to_string(),
        ]));
        proto.set_family_name(FAMILY_NAME.to_string());
        proto.set_family_version(FAMILY_VERSION.to_string());
        proto.set_inputs(protobuf::RepeatedField::from_vec(vec![
            KEY4.to_string(),
            (&KEY5[0..4]).to_string(),
        ]));
        proto.set_nonce(NONCE.to_string());
        proto.set_outputs(protobuf::RepeatedField::from_vec(vec![
            KEY6.to_string(),
            (&KEY7[0..4]).to_string(),
        ]));
        proto.set_payload_sha512(HASH.to_string());
        proto.set_signer_public_key(KEY8.to_string());

        b.iter(|| proto.clone().into_native());
    }
}
