//! The wire form of `Contract` after the sync with nodo dev 17e63ca4: the protocol is
//! field 2 and the xattrs are field 3.

use celaut_proto::celaut::contract::Protocol;
use celaut_proto::celaut::{BytesKeyValue, Contract};
use celaut_proto::Message;

#[test]
fn a_contract_keeps_its_protocol_and_xattrs() {
    let contract = Contract {
        protocol: Some(Protocol {
            tags: vec!["demo".into()],
            prose: "A kind of contract.".into(),
            formal: b"demo.protocol".to_vec(),
        }),
        xattrs: vec![BytesKeyValue {
            key: "script".into(),
            value: Some(vec![1, 2, 3]),
        }],
        ..Default::default()
    };
    let bytes = contract.encode_to_vec();
    let back = Contract::decode(bytes.as_slice()).unwrap();
    assert_eq!(back, contract);
    assert_eq!(back.protocol.unwrap().formal, b"demo.protocol");
    assert_eq!(back.xattrs.len(), 1);
}

#[test]
fn the_xattrs_of_a_contract_are_field_three() {
    let contract = Contract {
        xattrs: vec![BytesKeyValue {
            key: "k".into(),
            value: None,
        }],
        ..Default::default()
    };
    // Tag 3, wire type 2: 0x1a. The old field 2 was 0x12.
    assert_eq!(contract.encode_to_vec()[0], 0x1a);
}
