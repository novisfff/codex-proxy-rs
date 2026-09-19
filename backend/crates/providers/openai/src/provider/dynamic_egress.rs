//! 专用出口租约客户端。取消也释放租约；上游连接不复用、不重放。

use std::time::Duration;

use gateway_core::provider_ports::turn_state::DynamicEgressSelection;
use gateway_core::provider_ports::{ProviderStoreError, ProviderStoreErrorKind};
use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::DynamicEgressConfig;
use crate::transport::tls::build_reqwest_client_with_custom_ca;

#[derive(Clone)]
pub(super) struct DynamicEgress {
    http: Client,
    url: String,
    token: SecretString,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeaseResponse {
    state: String,
    ip: Option<String>,
    proxy_url: Option<String>,
    secret: Option<String>,
    provider: Option<String>,
    ip_verification: Option<String>,
}

pub(super) struct Lease {
    service: DynamicEgress,
    id: String,
    pub(super) ip: Option<String>,
    pub(super) http: Option<Client>,
    retain_ip: bool,
}

impl Lease {
    pub(super) fn retain_ip(&mut self) {
        self.retain_ip = true;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let service = self.service.clone();
        let id = self.id.clone();
        let retain_ip = self.retain_ip;
        tokio::spawn(async move {
            // 释放失败仍由远端租约超时关闭隧道；不能因此允许客户端回退。
            let _ = service
                .http
                .delete(format!("{}/v1/leases/{id}", service.url))
                .query(&[("retainIp", retain_ip)])
                .bearer_auth(service.token.expose_secret())
                .send()
                .await;
        });
    }
}

impl DynamicEgress {
    pub(super) async fn configure(&self, config: Value) -> Result<(), ProviderStoreError> {
        let response = self
            .http
            .put(format!("{}/v1/instances", self.url))
            .bearer_auth(self.token.expose_secret())
            .json(&config)
            .send()
            .await
            .map_err(|_| {
                ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, "dynamic egress")
            })?;
        if response.status().is_success() {
            return Ok(());
        }
        let kind = match response.status() {
            reqwest::StatusCode::CONFLICT => ProviderStoreErrorKind::Conflict,
            reqwest::StatusCode::BAD_REQUEST => ProviderStoreErrorKind::InvalidData,
            _ => ProviderStoreErrorKind::Unavailable,
        };
        Err(ProviderStoreError::new(kind, "dynamic egress"))
    }
    pub(super) fn new(config: &DynamicEgressConfig) -> Result<Self, ()> {
        let token = std::fs::read_to_string(&config.token_file).map_err(|_| ())?;
        if token.trim().len() < 32 {
            return Err(());
        }
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| ())?;
        Ok(Self {
            http,
            url: config.url.trim_end_matches('/').to_owned(),
            token: token.trim().to_owned().into(),
        })
    }

    pub(super) async fn status(&self) -> Value {
        let result = async {
            let response = self
                .http
                .get(format!("{}/v1/status", self.url))
                .bearer_auth(self.token.expose_secret())
                .send()
                .await
                .ok()?
                .error_for_status()
                .ok()?;
            let bytes = response.bytes().await.ok()?;
            if bytes.len() > 128 * 1024 {
                return None;
            }
            serde_json::from_slice::<Value>(&bytes).ok()
        }
        .await;
        result.unwrap_or_else(|| json!({"available":false,"message":"动态出口服务不可用","instances":[],"history":[]}))
    }

    pub(super) async fn acquire(&self, selection: &DynamicEgressSelection) -> Result<Lease, ()> {
        let mut lease = Lease {
            service: self.clone(),
            id: uuid::Uuid::new_v4().to_string(),
            ip: None,
            http: None,
            retain_ip: false,
        };
        let result = tokio::time::timeout(Duration::from_secs(900), async {
            loop {
                let response = self.http.post(format!("{}/v1/leases", self.url))
                    .bearer_auth(self.token.expose_secret())
                    .json(&json!({"id":lease.id,"instance":selection.instance,"family":selection.family}))
                    .send().await;
                let response = match response {
                    Ok(response) if response.status() == reqwest::StatusCode::CONFLICT => {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    Ok(response) => response.error_for_status().map_err(|_| ())?,
                    Err(_) => {
                        // 相同 attempt ID 重试控制面，不能分配另一组资源。
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let body: LeaseResponse = response.json().await.map_err(|_| ())?;
                match body.state.as_str() {
                    "provisioning" => tokio::time::sleep(Duration::from_secs(2)).await,
                    "ready" => {
                        let ip = lease_ip(&body, &selection.family)?;
                        let proxy = reqwest::Proxy::all(body.proxy_url.ok_or(())?).map_err(|_| ())?
                            .basic_auth(&lease.id, &body.secret.ok_or(())?);
                        lease.http = Some(build_reqwest_client_with_custom_ca(Client::builder()
                            .no_proxy().proxy(proxy).redirect(reqwest::redirect::Policy::none())
                            .retry(reqwest::retry::never()).http1_only().pool_max_idle_per_host(0)
                            .connect_timeout(Duration::from_secs(15))) .map_err(|_| ())?);
                        lease.ip = ip;
                        return Ok(());
                    }
                    _ => return Err(()),
                }
            }
        }).await.map_err(|_| ())?;
        result?;
        Ok(lease)
    }
}

fn lease_ip(body: &LeaseResponse, family: &str) -> Result<Option<String>, ()> {
    if body.provider.as_deref() == Some("novaproxy") {
        return if family == "ipv4"
            && body.ip.is_none()
            && body.ip_verification.as_deref() == Some("unverified")
        {
            Ok(None)
        } else {
            Err(())
        };
    }
    let ip: std::net::IpAddr = body.ip.as_deref().ok_or(())?.parse().map_err(|_| ())?;
    if ip.is_ipv4() != (family == "ipv4") {
        return Err(());
    }
    Ok(Some(ip.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn release_reports_retention_only_after_explicit_success() {
        for retain in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut lease = Lease {
                service: DynamicEgress {
                    http: Client::builder().no_proxy().build().unwrap(),
                    url: format!("http://{address}"),
                    token: "test-control-token".to_owned().into(),
                },
                id: "test-lease".to_owned(),
                ip: None,
                http: None,
                retain_ip: false,
            };
            if retain {
                lease.retain_ip();
            }
            drop(lease);
            tokio::time::timeout(Duration::from_secs(5), async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                while !data.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    data.extend_from_slice(&chunk[..count]);
                }
                let request = String::from_utf8(data).unwrap();
                assert!(request.starts_with(&format!(
                    "DELETE /v1/leases/test-lease?retainIp={retain} HTTP/1.1\r\n"
                )));
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
        }
    }

    #[test]
    fn novaproxy_requires_explicit_unverified_ipv4_lease() {
        let mut body: LeaseResponse = serde_json::from_value(
            json!({"state":"ready", "provider":"novaproxy", "ipVerification":"unverified"}),
        )
        .unwrap();
        assert_eq!(lease_ip(&body, "ipv4"), Ok(None));
        assert!(lease_ip(&body, "ipv6").is_err());
        body.ip = Some("203.0.113.1".to_owned());
        assert!(lease_ip(&body, "ipv4").is_err());
        body.ip = None;
        body.provider = Some("azure".to_owned());
        assert!(lease_ip(&body, "ipv4").is_err());
    }
}
