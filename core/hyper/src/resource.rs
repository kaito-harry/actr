//! Resource management
//!
//! Reserved scaffolding for future quota enforcement. The module is
//! compiled but no runtime consumer currently invokes it; items are
//! crate-private and tagged `allow(dead_code)`.

#![allow(dead_code)]

use actr_protocol::{ActorResult, ActrError};
use serde::{Deserialize, Serialize};

/// Resource quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceQuota {
    /// CPU quota（CPU cores）
    pub cpu_cores: f64,

    /// memory quota（bytes）
    pub memory_bytes: u64,

    /// network bandwidth quota（bytes/sec）
    pub network_bandwidth_bps: u64,

    /// disk IO quota（bytes/sec）
    pub disk_io_bps: u64,
}

impl Default for ResourceQuota {
    fn default() -> Self {
        Self {
            cpu_cores: 1.0,
            memory_bytes: 1024 * 1024 * 1024,         // 1GB
            network_bandwidth_bps: 100 * 1024 * 1024, // 100MB/s
            disk_io_bps: 100 * 1024 * 1024,           // 100MB/s
        }
    }
}

/// Resource usage
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    /// CPU usage rate (0.0-1.0)
    pub cpu_usage: f64,

    /// Memory used (bytes)
    pub memory_used_bytes: u64,

    /// Network bandwidth used (bytes/sec)
    pub network_usage_bps: u64,

    /// Disk IO used (bytes/sec)
    pub disk_io_bps: u64,
}

/// Resource configuration
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Whether to enforce resource limits
    pub enable_limits: bool,

    /// Resource monitoring interval (seconds)
    pub monitoring_interval_seconds: u64,

    /// Resource usage warning threshold (0.0-1.0)
    pub warning_threshold: f64,

    /// Resource usage hard-limit threshold (0.0-1.0)
    pub limit_threshold: f64,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            enable_limits: true,
            monitoring_interval_seconds: 5,
            warning_threshold: 0.8,
            limit_threshold: 0.95,
        }
    }
}

/// Resource manager
pub struct ResourceManager {
    config: ResourceConfig,
    quota: ResourceQuota,
    current_usage: ResourceUsage,
}

impl ResourceManager {
    /// Create a new resource manager
    pub fn new(config: ResourceConfig, quota: ResourceQuota) -> Self {
        Self {
            config,
            quota,
            current_usage: ResourceUsage::default(),
        }
    }

    /// Check whether the required resources are available
    pub fn check_resource_availability(&self, required: &ResourceUsage) -> ActorResult<bool> {
        if !self.config.enable_limits {
            return Ok(true);
        }

        // Check CPU
        let cpu_available =
            self.quota.cpu_cores - (self.current_usage.cpu_usage * self.quota.cpu_cores);
        if required.cpu_usage * self.quota.cpu_cores > cpu_available {
            return Ok(false);
        }

        // Check memory
        let memory_available = self.quota.memory_bytes - self.current_usage.memory_used_bytes;
        if required.memory_used_bytes > memory_available {
            return Ok(false);
        }

        Ok(true)
    }

    /// Allocate resources
    pub fn allocate_resources(&mut self, usage: &ResourceUsage) -> ActorResult<()> {
        if !self.check_resource_availability(usage)? {
            return Err(ActrError::Unavailable(
                "Insufficient resources available".to_string(),
            ));
        }

        self.current_usage.cpu_usage += usage.cpu_usage;
        self.current_usage.memory_used_bytes += usage.memory_used_bytes;
        self.current_usage.network_usage_bps += usage.network_usage_bps;
        self.current_usage.disk_io_bps += usage.disk_io_bps;

        Ok(())
    }

    /// Release resources
    pub fn release_resources(&mut self, usage: &ResourceUsage) -> ActorResult<()> {
        self.current_usage.cpu_usage = (self.current_usage.cpu_usage - usage.cpu_usage).max(0.0);
        self.current_usage.memory_used_bytes = self
            .current_usage
            .memory_used_bytes
            .saturating_sub(usage.memory_used_bytes);
        self.current_usage.network_usage_bps = self
            .current_usage
            .network_usage_bps
            .saturating_sub(usage.network_usage_bps);
        self.current_usage.disk_io_bps = self
            .current_usage
            .disk_io_bps
            .saturating_sub(usage.disk_io_bps);

        Ok(())
    }

    /// Get current resource usage
    pub fn get_usage(&self) -> &ResourceUsage {
        &self.current_usage
    }

    /// Get the resource quota
    pub fn get_quota(&self) -> &ResourceQuota {
        &self.quota
    }

    /// Compute the resource usage ratio
    pub fn calculate_usage_ratio(&self) -> ResourceUsageRatio {
        ResourceUsageRatio {
            cpu_ratio: self.current_usage.cpu_usage,
            memory_ratio: self.current_usage.memory_used_bytes as f64
                / self.quota.memory_bytes as f64,
            network_ratio: self.current_usage.network_usage_bps as f64
                / self.quota.network_bandwidth_bps as f64,
            disk_ratio: self.current_usage.disk_io_bps as f64 / self.quota.disk_io_bps as f64,
        }
    }
}

/// Resource usage ratio
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageRatio {
    /// CPU usage ratio (0.0-1.0)
    pub cpu_ratio: f64,

    /// Memory usage ratio (0.0-1.0)
    pub memory_ratio: f64,

    /// Network usage ratio (0.0-1.0)
    pub network_ratio: f64,

    /// Disk usage ratio (0.0-1.0)
    pub disk_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota(cpu: f64, mem: u64) -> ResourceQuota {
        ResourceQuota {
            cpu_cores: cpu,
            memory_bytes: mem,
            network_bandwidth_bps: 100,
            disk_io_bps: 100,
        }
    }

    #[test]
    fn quota_default_values() {
        let q = ResourceQuota::default();
        assert_eq!(q.cpu_cores, 1.0);
        assert_eq!(q.memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(q.network_bandwidth_bps, 100 * 1024 * 1024);
        assert_eq!(q.disk_io_bps, 100 * 1024 * 1024);
    }

    #[test]
    fn config_default_values() {
        let c = ResourceConfig::default();
        assert!(c.enable_limits);
        assert_eq!(c.monitoring_interval_seconds, 5);
        assert_eq!(c.warning_threshold, 0.8);
        assert_eq!(c.limit_threshold, 0.95);
    }

    #[test]
    fn resource_usage_default_is_zero() {
        let u = ResourceUsage::default();
        assert_eq!(u.cpu_usage, 0.0);
        assert_eq!(u.memory_used_bytes, 0);
        assert_eq!(u.network_usage_bps, 0);
        assert_eq!(u.disk_io_bps, 0);
    }

    #[test]
    fn disabled_limits_always_available() {
        let mut cfg = ResourceConfig::default();
        cfg.enable_limits = false;
        let rm = ResourceManager::new(cfg, ResourceQuota::default());

        // Even an absurd request is "available" when limits are off.
        let huge = ResourceUsage {
            cpu_usage: 1000.0,
            memory_used_bytes: u64::MAX,
            network_usage_bps: 0,
            disk_io_bps: 0,
        };
        assert!(rm.check_resource_availability(&huge).unwrap());
    }

    #[test]
    fn availability_ok_within_quota() {
        let rm = ResourceManager::new(ResourceConfig::default(), quota(4.0, 1024));
        let req = ResourceUsage {
            cpu_usage: 0.5, // 0.5 * 4 = 2 cores, available = 4 - 0 = 4
            memory_used_bytes: 512,
            network_usage_bps: 0,
            disk_io_bps: 0,
        };
        assert!(rm.check_resource_availability(&req).unwrap());
    }

    #[test]
    fn availability_rejected_on_cpu_exhaustion() {
        let rm = ResourceManager::new(ResourceConfig::default(), quota(1.0, 1024));
        let req = ResourceUsage {
            cpu_usage: 0.5, // available cpu = 1 - 0 = 1; required = 0.5*1 = 0.5 <= 1 ok here
            memory_used_bytes: 0,
            network_usage_bps: 0,
            disk_io_bps: 0,
        };
        // Sanity: passes with room to spare.
        assert!(rm.check_resource_availability(&req).unwrap());

        // Now pre-allocate to consume most CPU, then re-check should fail.
        let mut rm = rm;
        rm.allocate_resources(&ResourceUsage {
            cpu_usage: 0.9,
            memory_used_bytes: 0,
            network_usage_bps: 0,
            disk_io_bps: 0,
        })
        .unwrap();
        // available cpu = 1 - 0.9*1 = 0.1; required 0.5*1 = 0.5 > 0.1 → reject
        assert!(!rm.check_resource_availability(&req).unwrap());
    }

    #[test]
    fn availability_rejected_on_memory_exhaustion() {
        let rm = ResourceManager::new(ResourceConfig::default(), quota(1.0, 1000));
        let req = ResourceUsage {
            cpu_usage: 0.0,
            memory_used_bytes: 1500, // > 1000 available
            network_usage_bps: 0,
            disk_io_bps: 0,
        };
        assert!(!rm.check_resource_availability(&req).unwrap());
    }

    #[test]
    fn allocate_updates_usage_and_getters() {
        let mut rm = ResourceManager::new(ResourceConfig::default(), quota(2.0, 1024));
        let req = ResourceUsage {
            cpu_usage: 0.25,
            memory_used_bytes: 300,
            network_usage_bps: 10,
            disk_io_bps: 20,
        };
        rm.allocate_resources(&req).unwrap();

        let usage = rm.get_usage();
        assert_eq!(usage.cpu_usage, 0.25);
        assert_eq!(usage.memory_used_bytes, 300);
        assert_eq!(usage.network_usage_bps, 10);
        assert_eq!(usage.disk_io_bps, 20);

        let q = rm.get_quota();
        assert_eq!(q.cpu_cores, 2.0);
        assert_eq!(q.memory_bytes, 1024);
    }

    #[test]
    fn allocate_rejects_when_insufficient() {
        let mut rm = ResourceManager::new(ResourceConfig::default(), quota(1.0, 100));
        let req = ResourceUsage {
            cpu_usage: 0.0,
            memory_used_bytes: 200, // exceeds 100
            network_usage_bps: 0,
            disk_io_bps: 0,
        };
        let err = rm.allocate_resources(&req).unwrap_err();
        assert!(matches!(err, ActrError::Unavailable(_)));
        // Usage must remain unchanged on rejection.
        assert_eq!(rm.get_usage().memory_used_bytes, 0);
    }

    #[test]
    fn release_saturates_and_does_not_underflow() {
        let mut rm = ResourceManager::new(ResourceConfig::default(), quota(1.0, 1024));
        // Allocate a little, then release more than allocated — must saturate, not panic.
        rm.allocate_resources(&ResourceUsage {
            cpu_usage: 0.1,
            memory_used_bytes: 50,
            network_usage_bps: 5,
            disk_io_bps: 5,
        })
        .unwrap();
        rm.release_resources(&ResourceUsage {
            cpu_usage: 0.9,
            memory_used_bytes: 9999,
            network_usage_bps: 9999,
            disk_io_bps: 9999,
        })
        .unwrap();

        let u = rm.get_usage();
        assert_eq!(u.cpu_usage, 0.0);
        assert_eq!(u.memory_used_bytes, 0);
        assert_eq!(u.network_usage_bps, 0);
        assert_eq!(u.disk_io_bps, 0);
    }

    #[test]
    fn calculate_usage_ratio_reflects_allocation() {
        let mut rm = ResourceManager::new(
            ResourceConfig::default(),
            ResourceQuota {
                cpu_cores: 2.0,
                memory_bytes: 1000,
                network_bandwidth_bps: 200,
                disk_io_bps: 400,
            },
        );
        rm.allocate_resources(&ResourceUsage {
            cpu_usage: 0.5,         // cpu_ratio tracks raw usage fraction = 0.5
            memory_used_bytes: 250, // 250/1000 = 0.25
            network_usage_bps: 50,  // 50/200 = 0.25
            disk_io_bps: 100,       // 100/400 = 0.25
        })
        .unwrap();

        let r = rm.calculate_usage_ratio();
        assert_eq!(r.cpu_ratio, 0.5);
        assert!((r.memory_ratio - 0.25).abs() < f64::EPSILON);
        assert!((r.network_ratio - 0.25).abs() < f64::EPSILON);
        assert!((r.disk_ratio - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn ratio_zero_when_idle() {
        let rm = ResourceManager::new(ResourceConfig::default(), ResourceQuota::default());
        let r = rm.calculate_usage_ratio();
        assert_eq!(r.cpu_ratio, 0.0);
        assert_eq!(r.memory_ratio, 0.0);
        assert_eq!(r.network_ratio, 0.0);
        assert_eq!(r.disk_ratio, 0.0);
    }
}
