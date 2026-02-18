//! 全局兼容性缓存
//!
//! 在信令服务器内部维护一个内存缓存，存储兼容性检查结果。
//! 使用 actr-version 的 CompatibilityAnalysisResult 作为缓存值。

use actr_version::CompatibilityAnalysisResult;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tracing::{debug, info};

/// 兼容性缓存条目
#[derive(Debug, Clone)]
pub struct CompatibilityCacheEntry {
    /// 兼容性分析结果 (from actr-version)
    pub analysis_result: CompatibilityAnalysisResult,
    /// 缓存时间
    pub cached_at: SystemTime,
    /// 过期时间
    pub expires_at: SystemTime,
    /// 查询命中次数（统计）
    pub hit_count: u32,
}

/// 兼容性上报数据
#[derive(Debug, Clone)]
pub struct CompatibilityReportData {
    /// 源指纹（客户端期望的版本）
    pub from_fingerprint: String,
    /// 目标指纹（服务端提供的版本）
    pub to_fingerprint: String,
    /// 服务类型
    pub service_type: String,
    /// 兼容性分析结果
    pub analysis_result: CompatibilityAnalysisResult,
}

/// 兼容性缓存响应
#[derive(Debug, Clone)]
pub struct CompatibilityCacheResponse {
    /// 缓存键
    pub cache_key: String,
    /// 缓存的分析结果（如果存在）
    pub analysis_result: Option<CompatibilityAnalysisResult>,
    /// 是否命中缓存
    pub hit: bool,
}

/// 全局兼容性缓存管理器
#[derive(Debug)]
pub struct GlobalCompatibilityCache {
    /// 内存缓存 (cache_key -> entry)
    cache: HashMap<String, CompatibilityCacheEntry>,
    /// 最大缓存条目数
    max_entries: usize,
    /// 默认TTL（24小时）
    default_ttl: Duration,
}

impl GlobalCompatibilityCache {
    /// 创建新的缓存管理器
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            max_entries: 10000,
            default_ttl: Duration::from_secs(24 * 3600),
        }
    }

    /// 构建缓存键
    pub fn build_cache_key(
        service_type: &str,
        from_fingerprint: &str,
        to_fingerprint: &str,
    ) -> String {
        format!("{service_type}:{from_fingerprint}:{to_fingerprint}")
    }

    /// 查询兼容性缓存
    pub fn query(&mut self, cache_key: &str) -> CompatibilityCacheResponse {
        if let Some(entry) = self.cache.get_mut(cache_key) {
            if SystemTime::now() <= entry.expires_at {
                entry.hit_count += 1;
                debug!(
                    "兼容性缓存命中: {} (命中次数: {})",
                    cache_key, entry.hit_count
                );
                return CompatibilityCacheResponse {
                    cache_key: cache_key.to_string(),
                    analysis_result: Some(entry.analysis_result.clone()),
                    hit: true,
                };
            } else {
                debug!("兼容性缓存过期: {}", cache_key);
            }
        }

        debug!("兼容性缓存未命中: {}", cache_key);
        CompatibilityCacheResponse {
            cache_key: cache_key.to_string(),
            analysis_result: None,
            hit: false,
        }
    }

    /// 查询（不可变版本，不更新命中计数）
    pub fn query_readonly(&self, cache_key: &str) -> CompatibilityCacheResponse {
        if let Some(entry) = self.cache.get(cache_key)
            && SystemTime::now() <= entry.expires_at
        {
            debug!("兼容性缓存命中 (readonly): {}", cache_key);
            return CompatibilityCacheResponse {
                cache_key: cache_key.to_string(),
                analysis_result: Some(entry.analysis_result.clone()),
                hit: true,
            };
        }

        CompatibilityCacheResponse {
            cache_key: cache_key.to_string(),
            analysis_result: None,
            hit: false,
        }
    }

    /// 存储兼容性分析结果
    pub fn store(&mut self, report: CompatibilityReportData) {
        let cache_key = Self::build_cache_key(
            &report.service_type,
            &report.from_fingerprint,
            &report.to_fingerprint,
        );

        let now = SystemTime::now();
        let expires_at = now + self.default_ttl;

        if self.cache.len() >= self.max_entries {
            self.cleanup_expired();
        }

        if self.cache.len() >= self.max_entries
            && let Some(oldest_key) = self.find_oldest_entry()
        {
            self.cache.remove(&oldest_key);
            debug!("缓存已满，移除最旧条目: {}", oldest_key);
        }

        if let Some(existing) = self.cache.get_mut(&cache_key) {
            existing.analysis_result = report.analysis_result;
            existing.cached_at = now;
            existing.expires_at = expires_at;
            debug!("更新兼容性缓存: {}", cache_key);
        } else {
            let entry = CompatibilityCacheEntry {
                analysis_result: report.analysis_result,
                cached_at: now,
                expires_at,
                hit_count: 0,
            };
            self.cache.insert(cache_key.clone(), entry);
            info!("新增兼容性缓存: {}", cache_key);
        }
    }

    /// 清理过期条目
    pub fn cleanup_expired(&mut self) {
        let now = SystemTime::now();
        let before_count = self.cache.len();
        self.cache.retain(|_, entry| entry.expires_at > now);
        let removed = before_count - self.cache.len();
        if removed > 0 {
            info!("清理了 {} 个过期的兼容性缓存条目", removed);
        }
    }

    fn find_oldest_entry(&self) -> Option<String> {
        self.cache
            .iter()
            .min_by_key(|(_, entry)| entry.cached_at)
            .map(|(key, _)| key.clone())
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> CacheStats {
        let now = SystemTime::now();
        let total = self.cache.len();
        let expired = self.cache.values().filter(|e| e.expires_at <= now).count();
        let total_hits: u32 = self.cache.values().map(|e| e.hit_count).sum();

        CacheStats {
            total_entries: total,
            expired_entries: expired,
            total_hits,
            max_entries: self.max_entries,
        }
    }

    /// 获取指定指纹对的兼容性结果（用于 LoadBalancer）
    pub fn get_compatibility(
        &self,
        from_fingerprint: &str,
        to_fingerprint: &str,
    ) -> Option<&CompatibilityAnalysisResult> {
        let now = SystemTime::now();
        for (key, entry) in &self.cache {
            if key.contains(from_fingerprint)
                && key.contains(to_fingerprint)
                && entry.expires_at > now
            {
                return Some(&entry.analysis_result);
            }
        }
        None
    }
}

impl Default for GlobalCompatibilityCache {
    fn default() -> Self {
        Self::new()
    }
}

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_entries: usize,
    pub expired_entries: usize,
    pub total_hits: u32,
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_version::CompatibilityLevel;
    use std::time::UNIX_EPOCH;

    fn create_mock_analysis_result(level: CompatibilityLevel) -> CompatibilityAnalysisResult {
        CompatibilityAnalysisResult {
            level,
            changes: vec![],
            breaking_changes: vec![],
            base_semantic_fingerprint: "fp1".to_string(),
            candidate_semantic_fingerprint: "fp2".to_string(),
            analyzed_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_cache_store_and_query() {
        let mut cache = GlobalCompatibilityCache::new();

        let report = CompatibilityReportData {
            from_fingerprint: "client_fp".to_string(),
            to_fingerprint: "server_fp".to_string(),
            service_type: "test/service".to_string(),
            analysis_result: create_mock_analysis_result(CompatibilityLevel::FullyCompatible),
        };

        cache.store(report);

        let key =
            GlobalCompatibilityCache::build_cache_key("test/service", "client_fp", "server_fp");
        let response = cache.query(&key);

        assert!(response.hit);
        assert!(response.analysis_result.is_some());
        assert_eq!(
            response.analysis_result.unwrap().level,
            CompatibilityLevel::FullyCompatible
        );
    }

    #[test]
    fn test_cache_miss() {
        let mut cache = GlobalCompatibilityCache::new();
        let response = cache.query("nonexistent:key");
        assert!(!response.hit);
        assert!(response.analysis_result.is_none());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = GlobalCompatibilityCache::new();

        let report = CompatibilityReportData {
            from_fingerprint: "fp1".to_string(),
            to_fingerprint: "fp2".to_string(),
            service_type: "test/service".to_string(),
            analysis_result: create_mock_analysis_result(CompatibilityLevel::BackwardCompatible),
        };

        cache.store(report);
        let stats = cache.stats();

        assert_eq!(stats.total_entries, 1);
        assert_eq!(stats.expired_entries, 0);
    }

    #[test]
    fn test_cleanup_expired_removes_stale_entries() {
        let mut cache = GlobalCompatibilityCache::new();
        let report = CompatibilityReportData {
            from_fingerprint: "fp-clean-client".to_string(),
            to_fingerprint: "fp-clean-server".to_string(),
            service_type: "test/cleanup".to_string(),
            analysis_result: create_mock_analysis_result(CompatibilityLevel::FullyCompatible),
        };
        cache.store(report);

        let key = GlobalCompatibilityCache::build_cache_key(
            "test/cleanup",
            "fp-clean-client",
            "fp-clean-server",
        );
        let entry = cache
            .cache
            .get_mut(&key)
            .expect("cache entry should exist before cleanup");
        entry.expires_at = SystemTime::now() - Duration::from_secs(1);

        cache.cleanup_expired();
        let response = cache.query(&key);
        assert!(!response.hit, "expired entry should be removed");
        assert_eq!(cache.stats().total_entries, 0);
    }

    #[test]
    fn test_store_evicts_oldest_when_cache_is_full() {
        let mut cache = GlobalCompatibilityCache::new();
        cache.max_entries = 2;

        let report_1 = CompatibilityReportData {
            from_fingerprint: "fp-client-1".to_string(),
            to_fingerprint: "fp-server-1".to_string(),
            service_type: "test/evict".to_string(),
            analysis_result: create_mock_analysis_result(CompatibilityLevel::FullyCompatible),
        };
        cache.store(report_1);
        let key_1 =
            GlobalCompatibilityCache::build_cache_key("test/evict", "fp-client-1", "fp-server-1");
        cache.cache.get_mut(&key_1).expect("entry1").cached_at =
            UNIX_EPOCH + Duration::from_secs(1);

        let report_2 = CompatibilityReportData {
            from_fingerprint: "fp-client-2".to_string(),
            to_fingerprint: "fp-server-2".to_string(),
            service_type: "test/evict".to_string(),
            analysis_result: create_mock_analysis_result(CompatibilityLevel::BackwardCompatible),
        };
        cache.store(report_2);
        let key_2 =
            GlobalCompatibilityCache::build_cache_key("test/evict", "fp-client-2", "fp-server-2");
        cache.cache.get_mut(&key_2).expect("entry2").cached_at =
            UNIX_EPOCH + Duration::from_secs(2);

        let report_3 = CompatibilityReportData {
            from_fingerprint: "fp-client-3".to_string(),
            to_fingerprint: "fp-server-3".to_string(),
            service_type: "test/evict".to_string(),
            analysis_result: create_mock_analysis_result(CompatibilityLevel::FullyCompatible),
        };
        cache.store(report_3);
        let key_3 =
            GlobalCompatibilityCache::build_cache_key("test/evict", "fp-client-3", "fp-server-3");

        assert!(!cache.query(&key_1).hit, "oldest entry should be evicted");
        assert!(cache.query(&key_2).hit, "newer entry should remain");
        assert!(cache.query(&key_3).hit, "latest entry should remain");
        assert_eq!(
            cache.stats().total_entries,
            2,
            "cache should keep max_entries items"
        );
    }
}
