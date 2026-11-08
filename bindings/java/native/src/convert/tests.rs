use super::*;

#[test]
fn curve_key_z85_encodes_the_rfc_32_test_vector() {
    let block = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
    let key: [u8; 32] = std::array::from_fn(|index| block[index % block.len()]);
    assert_eq!(
        curve_key_z85(&key).as_deref(),
        Some("HelloWorldHelloWorldHelloWorldHelloWorld")
    );
}
