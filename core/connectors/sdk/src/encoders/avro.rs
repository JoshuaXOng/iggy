use crate::{Error, Payload, Schema, StreamEncoder, get_is_valid_avro};

pub struct AvroStreamEncoder;

impl StreamEncoder for AvroStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Avro
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        // Converting to Avro is currently limited as construction of `StreamEncoder`s
        // does not currently support any arguments. That's to say, we can't
        // reference any schema files if we wanted to / we'd have to derive Avro schema.
        match payload {
            Payload::Json(_) => Err(Error::InvalidPayloadType),
            Payload::Raw(bytes) => {
                if !get_is_valid_avro(&bytes) {
                    return Err(Error::InvalidAvroPayload);
                }
                Ok(bytes)
            }
            Payload::Text(_) => Err(Error::InvalidPayloadType),
            Payload::Proto(_) => Err(Error::InvalidPayloadType),
            Payload::FlatBuffer(_) => Err(Error::InvalidPayloadType),
            Payload::Avro(bytes) => {
                // Should be valid Avro bytes - but checking anyways.
                if !get_is_valid_avro(&bytes) {
                    return Err(Error::InvalidAvroPayload);
                }
                Ok(bytes)
            }
        }
    }
}
