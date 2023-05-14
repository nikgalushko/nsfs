use libc::{c_int, EEXIST, ENOENT, EOF};

#[derive(Debug)]
pub enum Error {
    NotFound,
    FileNotFound,
    AttrsNotFound,
    EOF,
    AlreadyExists,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => write!(f, "not found"),
            Error::AttrsNotFound => write!(f, "attributes not found"),
            Error::FileNotFound => write!(f, "file not found"),
            Error::EOF => write!(f, "eof"),
            Error::AlreadyExists => write!(f, "already exists"),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for c_int {
    fn from(value: Error) -> Self {
        match value {
            Error::NotFound | Error::AttrsNotFound | Error::FileNotFound => ENOENT,
            Error::EOF => EOF,
            Error::AlreadyExists => EEXIST,
        }
    }
}
