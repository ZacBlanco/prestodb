use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};


use crate::{
    err::error::Error,
    protocol::resources::{Base64Encoded, PlanFragment},
};

impl TryFrom<&Base64Encoded> for PlanFragment {
    type Error = Error;

    fn try_from(value: &Base64Encoded) -> Result<Self, Self::Error> {
        let mut result = Vec::new();
        if let Err(e) = BASE64_STANDARD_NO_PAD.decode_vec(value, &mut result) {
            return Err(Error::PlanDecode(format!("{:?}", e)));
        }

        serde_json::from_slice(&result).map_err(|e| Error::PlanDecode(format!("{:?}", e)))
    }
}
#[cfg(test)]
mod test {
    use std::{fs::read_to_string, path::Path};

    use crate::protocol::resources::{Base64Encoded, PlanFragment};

    #[test]
    fn test_deserialize_fragment() {
        let resources = Path::new(env!("CARGO_MANIFEST_DIR"));
        let input = resources.join(Path::new("test-resources/test-serialized-fragment.txt"));
        let output = resources.join(Path::new(
            "test-resources/test-serialized-fragment-deserialized.json",
        ));
        let input: Base64Encoded = read_to_string(input).unwrap() as Base64Encoded;
        let output = read_to_string(output).unwrap();
        let fragment = serde_json::from_str::<PlanFragment>(&output).unwrap();
        let plan: PlanFragment = (&input).try_into().unwrap();
        assert_eq!(plan, fragment);
    }
}
