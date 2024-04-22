use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use async_ssh2_lite::{AsyncSession, SessionConfiguration, TokioTcpStream};
use eyre::OptionExt;
use serde::Deserialize;

use crate::path_plain::PathPlainExt;
use crate::serde_extend::string::string_or_struct::{self, Void};
use crate::AResult;

#[derive(Debug, Clone, Deserialize)]
pub struct KeyPair {
    private_key: PathBuf,
    passphrase:  Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub enum Auth {
    #[serde(rename = "key-pair", with = "string_or_struct")]
    KeyPair(KeyPair),
    #[serde(rename = "password")]
    Password(String),
}

impl FromStr for KeyPair {
    type Err = Void;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(KeyPair {
            private_key: s.into(),
            passphrase:  None,
        })
    }
}

impl Auth {
    pub fn password(passwd: &str) -> Auth {
        Auth::Password(passwd.into())
    }

    pub fn key_pair<P: AsRef<Path>>(private_key: P, passphrase: Option<&str>) -> Auth {
        Auth::KeyPair(KeyPair {
            private_key: private_key.as_ref().into(),
            passphrase:  passphrase.map(|v| v.to_string()),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ssh {
    addr: SocketAddr,
    user: String,
    #[serde(rename = "auth")]
    auth: Auth,
}

impl Ssh {
    pub fn new<Addr>(addr: Addr, user: &str, auth: Auth) -> AResult<Self>
    where
        Addr: ToSocketAddrs + fmt::Display,
    {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_eyre(format!("error addr: {}", addr))?;
        Ok(Ssh {
            addr,
            user: user.into(),
            auth,
        })
    }

    pub async fn connect(&self) -> AResult<AsyncSession<TokioTcpStream>> {
        let mut session_configuration = SessionConfiguration::new();
        session_configuration.set_compress(true);
        let mut session =
            AsyncSession::<TokioTcpStream>::connect(self.addr, Some(session_configuration)).await?;
        session.handshake().await?;
        match &self.auth {
            Auth::KeyPair(KeyPair {
                private_key,
                passphrase,
            }) => {
                let private_key = private_key.plain()?;
                session
                    .userauth_pubkey_file(&self.user, None, &private_key, passphrase.as_deref())
                    .await?;
            },
            Auth::Password(password) => {
                session.userauth_password(&self.user, password).await?;
            },
        }
        Ok(session)
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}

#[cfg(test)]
mod tests {

    use std::fs;
    use std::path::Path;

    use tokio::io::AsyncWriteExt;

    use super::Ssh;
    use crate::ssh::connect::Auth;

    #[test]
    fn test_1() {
        let yaml_str = r#"
        !key-pair ~/.ssh/xxxx
        "#;
        let auth_method = serde_yaml::from_str::<Auth>(yaml_str);
        println!("{:?}", auth_method);

        let yaml_str = r#"
        !key-pair
            private_key: ~/.ssh/xxxxxxxx
        "#;
        let auth_method = serde_yaml::from_str::<Auth>(yaml_str);
        println!("{:?}", auth_method);

        let yaml_str = r#"
        !password asdfsdfsadfasdf
        "#;
        let auth_method = serde_yaml::from_str::<Auth>(yaml_str);
        println!("{:?}", auth_method);
    }

    #[test]
    fn test_2() {
        let toml_str = r#"
        key-pair = "~/.ssh/xxxx"
        "#;
        let auth_method = toml::from_str::<Auth>(toml_str);
        println!("{:?}", auth_method);

        let toml_str = r#"
        key-pair = {private_key = "~/.ssh/xxxx"}
        "#;
        let auth_method = toml::from_str::<Auth>(toml_str);
        println!("{:?}", auth_method);

        let toml_str = r#"
        password = "xxxxxxxxxxxxx"
        "#;
        let auth_method = toml::from_str::<Auth>(toml_str);
        println!("{:?}", auth_method);

        let toml_str = r#"
        password = "xxxxxxxxxxxxx"
        key-pair = "~/.ssh/xxxx"
        "#;
        let auth_method = toml::from_str::<Auth>(toml_str);
        println!("{:?}", auth_method);
    }

    #[tokio::test]
    async fn test_3() {
        let auth = Auth::key_pair("~/.ssh/id_ed25519-work", None);
        let ssh = Ssh::new("127.0.0.1:18822", "root", auth).unwrap();
        let session = ssh.connect().await.unwrap();
        let mut channel = session.channel_session().await.unwrap();
        let command = "mkdir -p /home/MySql-files";
        let r = channel.exec(command).await;
        println!("{:?}", r);

        let mut channel = session.channel_session().await.unwrap();
        let command = "mkdir -p /home/MySql-files/1";
        let r = channel.exec(command).await;
        println!("{:?}", r);

        let remote_path = Path::new("/home/MySql-files/1.txt");

        let file_bytes = fs::read("./_data/db-sql.toml").unwrap();

        let file_len = file_bytes.len();

        let mut channel = session
            .scp_send(remote_path, 0o644, file_len as u64, None)
            .await
            .unwrap();

        channel.write_all(&file_bytes).await.unwrap();

        // let mut channel = session.channel_session().await.unwrap();
        // let command = "rm -rf /home/MySql-files";
        // let r = channel.exec(command).await;
        // println!("{:?}", r);
    }
}
