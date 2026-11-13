//! Announcing the endpoint through the system's Avahi daemon (D-Bus), for
//! systems where avahi-daemon already owns mDNS.
//!
//! Avahi publishes the host name and every address of every interface and
//! keeps them current, so the announcement carries no address of its own and
//! no `ip` TXT property: the controller takes the address from the A records.
//! The registration is redone when avahi-daemon restarts, and the service is
//! renamed the way Avahi suggests on a name collision.

use crate::discovery::EndpointAnnouncement;
use futures_util::StreamExt;
use oaat_core::SERVICE_TYPE;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use zbus::zvariant::OwnedObjectPath;

const AVAHI_BUS_NAME: &str = "org.freedesktop.Avahi";
/// `AVAHI_IF_UNSPEC` / `AVAHI_PROTO_UNSPEC`: every interface, IPv4 and IPv6.
const UNSPEC: i32 = -1;
const RETRY_DELAY: Duration = Duration::from_secs(5);

// EntryGroup states (avahi-common/defs.h).
const GROUP_ESTABLISHED: i32 = 2;
const GROUP_COLLISION: i32 = 3;
const GROUP_FAILURE: i32 = 4;

#[zbus::proxy(
    interface = "org.freedesktop.Avahi.Server",
    default_service = "org.freedesktop.Avahi",
    default_path = "/"
)]
trait AvahiServer {
    fn entry_group_new(&self) -> zbus::Result<OwnedObjectPath>;
    fn get_alternative_service_name(&self, name: &str) -> zbus::Result<String>;
}

#[zbus::proxy(
    interface = "org.freedesktop.Avahi.EntryGroup",
    default_service = "org.freedesktop.Avahi"
)]
trait AvahiEntryGroup {
    #[allow(clippy::too_many_arguments)]
    fn add_service(
        &self,
        interface: i32,
        protocol: i32,
        flags: u32,
        name: &str,
        service_type: &str,
        domain: &str,
        host: &str,
        port: u16,
        txt: Vec<Vec<u8>>,
    ) -> zbus::Result<()>;
    fn commit(&self) -> zbus::Result<()>;
    fn reset(&self) -> zbus::Result<()>;

    #[zbus(signal)]
    fn state_changed(&self, state: i32, error: String) -> zbus::Result<()>;
}

/// Keeps the endpoint registered with Avahi until `shutdown` (or until the
/// process ends: Avahi drops a client's entry groups when it disconnects).
pub struct AvahiAnnouncer {
    task: JoinHandle<()>,
}

impl AvahiAnnouncer {
    /// Starts the registration in the background; never fails, it retries
    /// until Avahi is reachable.
    pub fn start(announcement: EndpointAnnouncement) -> Self {
        let task = tokio::spawn(async move {
            loop {
                if let Err(e) = announce(&announcement).await {
                    warn!(error = %e, "Avahi registration failed, retrying");
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        });
        AvahiAnnouncer { task }
    }

    pub fn shutdown(self) {
        // Dropping the task drops the D-Bus connection, which makes Avahi
        // withdraw the service.
        self.task.abort();
    }
}

fn txt_records(announcement: &EndpointAnnouncement) -> Vec<Vec<u8>> {
    announcement
        .txt_properties(None)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}").into_bytes())
        .collect()
}

/// Registers the service and stays registered. Returns when avahi-daemon goes
/// away or the registration fails, so the caller can start over.
async fn announce(announcement: &EndpointAnnouncement) -> zbus::Result<()> {
    let connection = zbus::Connection::system().await?;
    let dbus = zbus::fdo::DBusProxy::new(&connection).await?;
    let mut owner_changes = dbus
        .receive_name_owner_changed_with_args(&[(0, AVAHI_BUS_NAME)])
        .await?;

    let server = AvahiServerProxy::new(&connection).await?;
    let group_path = server.entry_group_new().await?;
    let group = AvahiEntryGroupProxy::builder(&connection)
        .path(group_path)?
        .build()
        .await?;
    let mut states = group.receive_state_changed().await?;

    let txt = txt_records(announcement);
    let mut name = announcement.instance_name.clone();
    add_and_commit(&group, &name, announcement.port, &txt).await?;

    loop {
        tokio::select! {
            change = owner_changes.next() => {
                // avahi-daemon stopped or restarted: its entry groups are gone.
                if change.is_none() {
                    return Err(zbus::Error::Failure("D-Bus signal stream ended".into()));
                }
                info!("avahi-daemon went away, registering again");
                return Ok(());
            }
            signal = states.next() => {
                let Some(signal) = signal else {
                    return Err(zbus::Error::Failure("D-Bus signal stream ended".into()));
                };
                let args = signal.args()?;
                match args.state {
                    GROUP_ESTABLISHED => info!(
                        name = %name,
                        port = announcement.port,
                        service_type = SERVICE_TYPE,
                        "mDNS service registered with Avahi"
                    ),
                    GROUP_COLLISION => {
                        let alternative = server.get_alternative_service_name(&name).await?;
                        warn!(name = %name, alternative = %alternative, "mDNS name collision, renaming");
                        name = alternative;
                        group.reset().await?;
                        add_and_commit(&group, &name, announcement.port, &txt).await?;
                    }
                    GROUP_FAILURE => {
                        return Err(zbus::Error::Failure(format!(
                            "Avahi entry group failed: {}",
                            args.error
                        )));
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn add_and_commit(
    group: &AvahiEntryGroupProxy<'_>,
    name: &str,
    port: u16,
    txt: &[Vec<u8>],
) -> zbus::Result<()> {
    group
        .add_service(
            UNSPEC,
            UNSPEC,
            0,
            name,
            SERVICE_TYPE,
            "",
            "",
            port,
            txt.to_vec(),
        )
        .await?;
    group.commit().await
}
