use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use async_ssh2_lite::{AsyncChannel, TokioTcpStream};
use eyre::{Error, OptionExt};
use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::connect::{Auth, Ssh};
use crate::eyre_ext::EyreExt;
use crate::AResult;

pub enum ForwarderMessage {
    LocalAcceptError(Error),
    LocalAcceptSuccess(SocketAddr),
    LocalReadEof(SocketAddr),
    TunnelChannelReadEof(SocketAddr),
    Error((SocketAddr, Error)),
}

#[derive(Debug, Clone)]
pub struct SshTunnel {
    ssh:         Ssh,
    target_addr: SocketAddr,
}

impl SshTunnel {
    pub fn new<Addr1, Addr2>(
        tunnel_addr: Addr1,
        tunnel_user: &str,
        tunnel_auth_method: Auth,
        target_addr: Addr2,
    ) -> AResult<Self>
    where
        Addr1: ToSocketAddrs + fmt::Display,
        Addr2: ToSocketAddrs + fmt::Display,
    {
        let ssh = Ssh::new(tunnel_addr, tunnel_user, tunnel_auth_method)?;
        let target_addr = target_addr
            .to_socket_addrs()?
            .next()
            .ok_or_eyre(format!("error addr:{}", target_addr))?;
        Ok(SshTunnel { ssh, target_addr })
    }

    pub fn new_by_ssh<Addr>(ssh: Ssh, target_addr: Addr) -> AResult<Self>
    where
        Addr: ToSocketAddrs + fmt::Display,
    {
        let target_addr = target_addr
            .to_socket_addrs()?
            .next()
            .ok_or_eyre(format!("error addr:{}", target_addr))?;
        Ok(SshTunnel { ssh, target_addr })
    }

    // ssh -L 127.0.0.1:13306:192.168.31.155:3306 -p 11122 Administrator@127.0.0.1 -N
    async fn connect_ssh_and_channel_direct_tcpip(&self) -> AResult<AsyncChannel<TokioTcpStream>> {
        let tunnel_session = self.ssh.connect().await?;

        let tunnel_channel = tunnel_session
            .channel_direct_tcpip(
                &self.target_addr.ip().to_string(),
                self.target_addr.port(),
                None,
            )
            .await?;
        Ok(tunnel_channel)
    }

    async fn spawn_channel_streamers(
        mut tunnel_channel: AsyncChannel<TokioTcpStream>,
        mut forward_stream_r: TokioTcpStream,
        sender: UnboundedSender<ForwarderMessage>,
        addr: SocketAddr,
    ) -> AResult<()> {
        let mut buf_tunnel_channel = vec![0; 2048];
        let mut buf_forward_stream_r = vec![0; 2048];

        loop {
            tokio::select! {
                ret_forward_stream_r = forward_stream_r.read(&mut buf_forward_stream_r) => match ret_forward_stream_r {
                    Ok(0) => {
                        sender.send(ForwarderMessage::LocalReadEof(addr))?;
                        break;
                    },
                    Ok(n) => {
                        tunnel_channel.write(&buf_forward_stream_r[..n]).await.eyre_with_msg("local to tunnel channel write")?;
                    },
                    e @ Err(_) => {
                        e.eyre_with_msg("local read")?;
                    }
                },
                ret_tunnel_channel = tunnel_channel.read(&mut buf_tunnel_channel) => match ret_tunnel_channel {
                    Ok(0) =>{
                        sender.send(ForwarderMessage::TunnelChannelReadEof(addr))?;
                        break;
                    },
                    Ok(n) => {
                        forward_stream_r.write(&buf_tunnel_channel[..n]).await.eyre_with_msg("tunnel channel to local wirte")?;
                    },
                    e @ Err(_)=>{
                        e.eyre_with_msg("tunnel_channel read")?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn open_tunnel(&self) -> AResult<(u16, UnboundedReceiver<ForwarderMessage>)> {
        let mut channel = self.connect_ssh_and_channel_direct_tcpip().await?;
        channel.close().await?;

        let listen_addr = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let listener = TcpListener::bind(listen_addr).await?;
        let (sender, receiver) = mpsc::unbounded_channel();
        // let (sender, receiver) = async_channel::unbounded();
        let this = Arc::new(self.clone());

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((forward_stream_r, addr)) => {
                        sender
                            .send(ForwarderMessage::LocalAcceptSuccess(addr))
                            .unwrap();
                        let this = this.clone();
                        let sender = sender.clone();
                        tokio::spawn(async move {
                            let sender_inner = sender.clone();
                            let r = tokio::spawn(async move {
                                let tunnel_channel =
                                    this.connect_ssh_and_channel_direct_tcpip().await?;
                                Self::spawn_channel_streamers(
                                    tunnel_channel,
                                    forward_stream_r,
                                    sender_inner,
                                    addr,
                                )
                                .await?;
                                Result::<(), Error>::Ok(())
                            })
                            .await
                            .unwrap();
                            if let Err(e) = r {
                                sender.send(ForwarderMessage::Error((addr, e))).unwrap();
                            }
                        });
                    },
                    Err(e) => {
                        sender
                            .send(ForwarderMessage::LocalAcceptError(e.into()))
                            .unwrap();
                    },
                }
            }
        });

        debug!("[ssh-tunnel] listen on {}", listen_addr);

        Ok((listen_addr.port(), receiver))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Local;

    async fn print(flag: &str) {
        let now = Local::now().naive_local();
        println!("{} {}", now, flag);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn test_1() {
        let join_handle1 = tokio::spawn(async move {
            print("1").await;
        });
        let join_handle2 = tokio::spawn(async move {
            print("2").await;
        });
        let join_handle3 = tokio::spawn(async move {
            print("3").await;
        });
        join_handle1.await.unwrap();
        join_handle2.await.unwrap();
        join_handle3.await.unwrap();
    }
}
