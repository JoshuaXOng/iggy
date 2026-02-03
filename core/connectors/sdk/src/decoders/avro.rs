use crate::{Error, Payload, Schema, StreamDecoder, get_is_valid_avro};

pub struct AvroStreamDecoder;

impl StreamDecoder for AvroStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Avro
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        if !get_is_valid_avro(&payload) {
            return Err(Error::InvalidAvroPayload);
        }

        Ok(Payload::Avro(payload))
    }
}
