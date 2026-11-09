//! The error type of the broker adapters.

/// The kind of an [`Error`].
///
/// The Python bindings select the Python exception class from the kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The topic or the subscription does not exist.
    NotFound,
    /// The topic or the subscription exists already.
    AlreadyExists,
    /// The caller gave a value that is not valid, for example a topic path
    /// with a wrong format.
    InvalidArgument,
    /// Each other error, for example a network error or a missing
    /// permission.
    Other,
}

/// An error of a broker adapter.
///
/// The error holds its [`ErrorKind`] and the text from the broker client.
///
/// # Examples
///
/// ```
/// use fastpubsub_messaging::{Error, ErrorKind};
///
/// let error = Error::new(ErrorKind::NotFound, "the topic does not exist");
/// assert_eq!(error.kind(), ErrorKind::NotFound);
/// assert_eq!(error.to_string(), "the topic does not exist");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    /// The kind of the error.
    kind: ErrorKind,
    /// The text from the broker client.
    message: String,
}

impl Error {
    /// Make an error with a kind and a text.
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Make an error of the kind [`ErrorKind::Other`].
    #[must_use]
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other, message)
    }

    /// Get the kind of the error.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

/// The result type of the broker adapters.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::{Error, ErrorKind};

    #[test]
    fn other_has_the_other_kind_and_the_text() {
        let error = Error::other("connection refused");

        assert_eq!(error.kind(), ErrorKind::Other);
        assert_eq!(error.to_string(), "connection refused");
    }
}
