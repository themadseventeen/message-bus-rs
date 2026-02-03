use rand::prelude::*;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub(crate) u64);

#[derive(Error, Debug)]
pub enum ClientIdError {
    #[error("invalid lenght")]
    InvalidLength,
    #[error("invalid character")]
    InvalidCharacter,
}

impl TryFrom<&str> for ClientId {
    type Error = ClientIdError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.len() != 16 {
            return Err(ClientIdError::InvalidLength);
        }

        let value = u64::from_str_radix(s, 16).map_err(|_| ClientIdError::InvalidCharacter)?;

        Ok(ClientId(value))
    }
}

impl TryFrom<&String> for ClientId {
    type Error = ClientIdError;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        ClientId::try_from(s.as_str())
    }
}

impl TryFrom<String> for ClientId {
    type Error = ClientIdError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        ClientId::try_from(s.as_str())
    }
}

impl From<u64> for ClientId {
    fn from(v: u64) -> Self {
        ClientId(v)
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self(rand::rng().random::<u64>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_string() {
        let mut s = String::from("deadbeefdeadbeef");
        assert!(ClientId::try_from(&s).is_ok());
        assert!(ClientId::try_from(s.as_str()).is_ok());
        s = String::from("1234567890abcdef");
        assert!(ClientId::try_from(&s).is_ok());
        assert!(ClientId::try_from(s.as_str()).is_ok());
        s = String::from("wrongcharset1234");
        assert!(ClientId::try_from(&s).is_err());
        assert!(ClientId::try_from(s.as_str()).is_err());
        s = String::from("deadbeefdead");
        assert!(ClientId::try_from(&s).is_err());
        assert!(ClientId::try_from(s.as_str()).is_err());
    }
}
