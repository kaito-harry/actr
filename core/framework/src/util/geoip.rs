//! GeoIP geolocation lookup utility
//!
//! Provides IP-to-geographic-coordinate conversion based on MaxMind GeoLite2 database
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use actr_framework::util::geoip::GeoIpService;
//! use std::net::IpAddr;
//!
//! // Initialize GeoIP service
//! let geoip = GeoIpService::new("data/geoip/GeoLite2-City.mmdb")?;
//!
//! // Query coordinates for an IP address
//! let ip: IpAddr = "8.8.8.8".parse()?;
//! if let Some((lat, lon)) = geoip.lookup(ip) {
//!     println!("IP {} is at coordinates: ({}, {})", ip, lat, lon);
//! }
//! ```
//!
//! # Obtaining the GeoLite2 Database
//!
//! **Auto-download (recommended):**
//! 1. Visit https://www.maxmind.com/en/geolite2/signup to get a License Key
//! 2. Set environment variable: `export MAXMIND_LICENSE_KEY="your-key-here"`
//! 3. Auto-downloaded on first call to `GeoIpService::new()`
//!
//! **Manual download (production):**
//! ```bash
//! curl -o GeoLite2-City.tar.gz \
//!   "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=YOUR_KEY&suffix=tar.gz"
//! tar -xzf GeoLite2-City.tar.gz --strip-components=1 -C data/geoip/ "*/GeoLite2-City.mmdb"
//! ```

#[cfg(feature = "geoip")]
use anyhow::{Context, Result};
#[cfg(feature = "geoip")]
use maxminddb::{Reader, geoip2::City};
#[cfg(feature = "geoip")]
use std::net::IpAddr;
#[cfg(feature = "geoip")]
use std::path::Path;
#[cfg(feature = "geoip")]
use tracing::{debug, info, warn};

/// GeoIP lookup service
///
/// Provides IP-to-geographic-coordinate conversion
#[cfg(feature = "geoip")]
#[derive(Debug)]
pub struct GeoIpService {
    reader: Reader<Vec<u8>>,
}

#[cfg(feature = "geoip")]
impl GeoIpService {
    /// Initialize GeoIP service (supports auto-download)
    ///
    /// # Arguments
    /// * `db_path` - Path to GeoLite2-City.mmdb database file
    ///
    /// # Errors
    /// Returns error if database file does not exist or has invalid format
    ///
    /// # Auto-download
    /// If the database file does not exist and `MAXMIND_LICENSE_KEY` env var is set,
    /// GeoLite2-City database will be automatically downloaded from MaxMind.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let path = db_path.as_ref();

        // If database does not exist, attempt auto-download
        if !path.exists() {
            info!("GeoIP database not found at {:?}", path);

            if let Ok(license_key) = std::env::var("MAXMIND_LICENSE_KEY") {
                info!("MAXMIND_LICENSE_KEY found, attempting auto-download...");
                Self::download_database(path, &license_key)?;
            } else {
                anyhow::bail!(
                    "GeoIP database not found at {:?}\n\
                     \n\
                     To auto-download:\n\
                     1. Get License Key: https://www.maxmind.com/en/geolite2/signup\n\
                     2. export MAXMIND_LICENSE_KEY=\"your-key\"\n\
                     3. Retry\n\
                     \n\
                     Or manually download:\n\
                     curl -o GeoLite2-City.tar.gz \\\n\
                       'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=YOUR_KEY&suffix=tar.gz'\n\
                     tar -xzf GeoLite2-City.tar.gz --strip-components=1 -C {:?}/ '*/GeoLite2-City.mmdb'",
                    path,
                    path.parent().unwrap_or(Path::new("."))
                );
            }
        }

        info!("Loading GeoIP database from: {:?}", path);
        let reader = Reader::open_readfile(path)
            .context(format!("Failed to open GeoIP database at {path:?}"))?;

        info!(
            "GeoIP service initialized (build epoch: {})",
            reader.metadata.build_epoch
        );
        Ok(Self { reader })
    }

    /// Auto-download GeoLite2-City database
    fn download_database(db_path: &Path, license_key: &str) -> Result<()> {
        use reqwest::blocking::Client;

        info!("Downloading GeoLite2-City database (~70MB)...");

        // Build download URL
        let url = format!(
            "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key={}&suffix=tar.gz",
            license_key
        );

        // Download
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // 5 minute timeout
            .build()?;

        let response = client
            .get(&url)
            .send()
            .context("Failed to download GeoLite2 database")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Download failed with status: {} - Check your MAXMIND_LICENSE_KEY",
                response.status()
            );
        }

        info!("Download complete, extracting...");

        // Decompress tar.gz
        let tar_gz_data = response.bytes()?;
        let tar_decoder = flate2::read::GzDecoder::new(&tar_gz_data[..]);
        let mut archive = tar::Archive::new(tar_decoder);

        // Find and extract the .mmdb file
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path_in_archive = entry.path()?;

            if path_in_archive.extension() == Some(std::ffi::OsStr::new("mmdb"))
                && path_in_archive.to_string_lossy().contains("GeoLite2-City")
            {
                // Create parent directory
                if let Some(parent) = db_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Extract to target location
                let mut output = std::fs::File::create(db_path)?;
                std::io::copy(&mut entry, &mut output)?;

                let size = std::fs::metadata(db_path)?.len();
                info!(
                    "GeoIP database downloaded to {:?} ({:.1} MB)",
                    db_path,
                    size as f64 / 1_048_576.0
                );
                return Ok(());
            }
        }

        anyhow::bail!("GeoLite2-City.mmdb not found in downloaded archive");
    }

    /// Look up geographic coordinates for an IP address
    ///
    /// # Arguments
    /// * `ip` - The IP address to look up
    ///
    /// # Returns
    /// * `Some((latitude, longitude))` - Coordinates found
    /// * `None` - IP not in database or no coordinate info
    pub fn lookup(&self, ip: IpAddr) -> Option<(f64, f64)> {
        match self
            .reader
            .lookup(ip)
            .and_then(|result| result.decode::<City>())
        {
            Ok(Some(city)) => {
                let location = city.location;
                if let (Some(lat), Some(lon)) = (location.latitude, location.longitude) {
                    debug!("GeoIP lookup: {} -> ({}, {})", ip, lat, lon);
                    return Some((lat, lon));
                }
                debug!("GeoIP lookup: {} found but no coordinates", ip);
                None
            }
            Ok(None) => {
                debug!("GeoIP lookup: {} not in database", ip);
                None
            }
            Err(e) => {
                warn!("GeoIP lookup error for {}: {}", ip, e);
                None
            }
        }
    }

    /// Get database metadata
    pub fn metadata(&self) -> &maxminddb::Metadata {
        &self.reader.metadata
    }
}

/// Fallback implementation when GeoIP feature is disabled
#[cfg(not(feature = "geoip"))]
#[derive(Debug)]
pub struct GeoIpService;

#[cfg(not(feature = "geoip"))]
impl GeoIpService {
    /// Initialization fails (requires geoip feature)
    pub fn new<P>(_db_path: P) -> anyhow::Result<Self> {
        anyhow::bail!("GeoIP feature is not enabled. Rebuild with --features geoip")
    }

    /// Always returns None (requires geoip feature)
    pub fn lookup(&self, _ip: std::net::IpAddr) -> Option<(f64, f64)> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geoip_module_compiles() {
        let _ = std::mem::size_of::<GeoIpService>();
    }

    #[cfg(feature = "geoip")]
    #[test]
    fn test_geoip_lookup_requires_database() {
        // Test requires a real database file; here we only verify API availability
        let result = GeoIpService::new("/nonexistent/path.mmdb");
        assert!(result.is_err());
    }

    #[cfg(not(feature = "geoip"))]
    #[test]
    fn test_geoip_feature_disabled() {
        let result = GeoIpService::new("/any/path.mmdb");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("GeoIP feature is not enabled")
        );
    }
}
