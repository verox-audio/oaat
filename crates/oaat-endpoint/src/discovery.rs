use mdns_sd::{ServiceDaemon, ServiceInfo};
use oaat_core::capability::Capabilities;
use oaat_core::{DEFAULT_CONTROL_PORT, PROTOCOL_VERSION, SERVICE_TYPE};
use std::net::Ipv4Addr;
use tracing::{info, warn};

pub struct EndpointAnnouncement {
    pub instance_name: String,
    pub port: u16,
    pub endpoint_id: String,
    pub capabilities: Capabilities,
    pub channels_max: u8,
    pub volume_type: Option<String>,
    pub model: Option<String>,
    pub vendor: Option<String>,
    pub firmware: Option<String>,
}

fn detect_local_ip() -> Ipv4Addr {
    if let Some(sock) = std::net::UdpSocket::bind("0.0.0.0:0").ok()
        && sock.connect("8.8.8.8:53").is_ok()
        && let Ok(addr) = sock.local_addr()
        && let std::net::IpAddr::V4(v4) = addr.ip()
    {
        return v4;
    }
    warn!("could not detect local IP, falling back to 0.0.0.0");
    Ipv4Addr::UNSPECIFIED
}

impl EndpointAnnouncement {
    pub fn service_type() -> &'static str {
        SERVICE_TYPE
    }

    pub fn default_port() -> u16 {
        DEFAULT_CONTROL_PORT
    }

    /// TXT properties of the announcement. `ip` is only included when given:
    /// it tells the controller which address to use, so it has to be current.
    pub fn txt_properties(&self, ip: Option<&str>) -> Vec<(String, String)> {
        let mut props = vec![
            ("v".to_string(), PROTOCOL_VERSION.to_string()),
            ("id".to_string(), self.endpoint_id.clone()),
            ("name".to_string(), self.instance_name.clone()),
            ("caps".to_string(), self.capabilities.to_string()),
            ("ch".to_string(), self.channels_max.to_string()),
        ];
        if let Some(ip) = ip {
            props.push(("ip".to_string(), ip.to_string()));
        }
        for (key, value) in [
            ("vol", &self.volume_type),
            ("model", &self.model),
            ("vendor", &self.vendor),
            ("fw", &self.firmware),
        ] {
            if let Some(value) = value {
                props.push((key.to_string(), value.clone()));
            }
        }
        props
    }

    pub fn register(&self, mdns: &ServiceDaemon) -> Result<(), mdns_sd::Error> {
        let service_type = format!("{}.local.", SERVICE_TYPE);
        let hostname = format!(
            "{}.local.",
            hostname::get().unwrap_or_default().to_string_lossy()
        );

        let local_ip = detect_local_ip();
        let ip_str = local_ip.to_string();
        let owned = self.txt_properties(Some(&ip_str));
        let props: Vec<(&str, &str)> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let service = ServiceInfo::new(
            &service_type,
            &self.instance_name,
            &hostname,
            &ip_str,
            self.port,
            &props[..],
        )?;

        mdns.register(service)?;
        info!(
            name = %self.instance_name,
            port = self.port,
            ip = %local_ip,
            service_type = SERVICE_TYPE,
            "mDNS service registered"
        );
        Ok(())
    }
}

/// Keeps the endpoint announced on the network until `shutdown`.
pub enum Announcer {
    /// The built-in mDNS responder (mdns-sd).
    MdnsSd(ServiceDaemon),
    /// The system's Avahi daemon, over D-Bus.
    #[cfg(feature = "mdns-avahi")]
    Avahi(crate::avahi::AvahiAnnouncer),
}

impl Announcer {
    /// Announces through Avahi when built with `mdns-avahi`, otherwise with
    /// the built-in responder. Avahi mode never starts the built-in responder:
    /// two mDNS stacks on one host make mDNS unreliable. If Avahi isn't
    /// reachable yet, it keeps retrying in the background.
    pub fn start(announcement: EndpointAnnouncement) -> Result<Self, mdns_sd::Error> {
        #[cfg(feature = "mdns-avahi")]
        {
            Ok(Announcer::Avahi(crate::avahi::AvahiAnnouncer::start(
                announcement,
            )))
        }
        #[cfg(not(feature = "mdns-avahi"))]
        {
            let mdns = ServiceDaemon::new()?;
            announcement.register(&mdns)?;
            Ok(Announcer::MdnsSd(mdns))
        }
    }

    pub fn shutdown(self) {
        match self {
            Announcer::MdnsSd(mdns) => {
                let _ = mdns.shutdown();
            }
            #[cfg(feature = "mdns-avahi")]
            Announcer::Avahi(avahi) => avahi.shutdown(),
        }
    }
}
