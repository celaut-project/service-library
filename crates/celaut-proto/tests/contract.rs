//! The wire form of `Contract` and `Configuration` after the sync with nodo dev. The protocol
//! of a contract is field 2 and its xattrs are field 3. The initial_mu of a configuration
//! is field 2.

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

#[test]
fn the_initial_mu_of_a_configuration_is_field_two() {
    use celaut_proto::celaut::{Amount, Configuration};

    let configuration = Configuration {
        environment_variables: Vec::new(),
        initial_mu: Some(Amount { n: "7".into() }),
        offer: None,
    };
    let bytes = configuration.encode_to_vec();
    // Tag 2, wire type 2: 0x12. The old field 3 was 0x1a. The Amount holds the string "7"
    // as field 1: 0x0a 0x01 0x37.
    assert_eq!(bytes, [0x12, 0x03, 0x0a, 0x01, 0x37]);
    let back = Configuration::decode(bytes.as_slice()).unwrap();
    assert_eq!(back, configuration);
}

#[test]
fn the_offer_of_a_configuration_is_field_three() {
    use celaut_proto::celaut::{Configuration, EstimatedCost};

    let configuration = Configuration {
        offer: Some(EstimatedCost {
            valid_until: 5,
            signature: "ab".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let bytes = configuration.encode_to_vec();
    assert_eq!(bytes[0], 0x1a);
    let back = Configuration::decode(bytes.as_slice()).unwrap();
    assert_eq!(back, configuration);
}
