use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Invalid credentials")]
pub struct Error;

/// Database connection credentials from the connection string
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Credentials {
    pub user: Bytes,
    pub database: Bytes,
    pub password: Bytes,
}

impl Credentials {
    pub fn build() -> CredentialsBuilder {
        CredentialsBuilder::default()
    }
}

#[derive(Default)]
pub struct CredentialsBuilder {
    user: Option<Bytes>,
    database: Option<Bytes>,
    password: Option<Bytes>,
}

impl CredentialsBuilder {
    pub fn user(&mut self, user: Bytes) {
        self.user = Some(user);
    }

    pub fn database(&mut self, database: Bytes) {
        self.database = Some(database);
    }

    pub fn password(&mut self, password: Bytes) {
        self.password = Some(password);
    }

    pub fn finish(self) -> Result<Credentials, Error> {
        let user = self.user.ok_or(Error)?;
        let password = self.password.ok_or(Error)?;
        let database = self.database.unwrap_or_else(|| user.to_owned());

        Ok(Credentials {
            user,
            password,
            database,
        })
    }
}
