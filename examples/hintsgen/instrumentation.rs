// SPDX-License-Identifier: MIT OR Apache-2.0

//! Persistent metrics, allocator heatmaps, fault-address manifests, and final run reporting.
//!
//! [`Instrumentation`] owns the output directory and writes a consistent before/after compaction
//! snapshot. [`StorageStats`] distinguishes sparse logical size from allocated filesystem blocks.
//! Linux mapping manifests include the process ID because virtual addresses from another process
//! cannot be attributed safely even when their ranges overlap.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use floresta_db::{BatchPopStats, DatabaseStats, Error, PageStats};

use super::body::HintsStats;
use super::{AnyResult, WorkerStats, invalid_input, log_progress, path_text};

/// Filesystem size totals for one file or recursively for one directory.
#[derive(Clone, Copy, Default)]
pub(super) struct StorageStats {
    pub(super) files: u64,
    pub(super) logical_bytes: u64,
    pub(super) allocated_bytes: u64,
}

impl StorageStats {
    /// Measures logical length and physically allocated blocks below `path`.
    pub(super) fn read(path: &Path) -> io::Result<Self> {
        let metadata = path.metadata()?;
        if metadata.is_file() {
            return Ok(Self {
                files: 1,
                logical_bytes: metadata.len(),
                allocated_bytes: metadata.blocks().saturating_mul(512),
            });
        }
        let mut total = Self::default();
        for entry in std::fs::read_dir(path)? {
            total.merge(Self::read(&entry?.path())?);
        }
        Ok(total)
    }

    fn merge(&mut self, other: Self) {
        self.files = self.files.saturating_add(other.files);
        self.logical_bytes = self.logical_bytes.saturating_add(other.logical_bytes);
        self.allocated_bytes = self.allocated_bytes.saturating_add(other.allocated_bytes);
    }
}

/// Owns all machine-readable and visual instrumentation artifacts for one run.
pub(super) struct Instrumentation {
    directory: PathBuf,
}

impl Instrumentation {
    /// Creates the artifact directory and captures the database's initial virtual mappings.
    pub(super) fn create(work_directory: &Path, database_path: &Path) -> AnyResult<Self> {
        let directory = work_directory.join("instrumentation");
        std::fs::create_dir(&directory)?;
        Self::write_mapping_manifest(&directory.join("mappings.csv"), database_path)?;
        Ok(Self { directory })
    }

    /// Writes allocator and traversal metrics captured around compaction.
    pub(super) fn write_database_snapshot(
        &self,
        before: &DatabaseStats,
        after: &DatabaseStats,
        storage_before: StorageStats,
        storage_after: StorageStats,
        removers: &WorkerStats,
    ) -> AnyResult<()> {
        Self::write_metrics_summary(
            &self.directory.join("summary.csv"),
            before,
            after,
            storage_before,
            storage_after,
            removers,
        )?;
        Self::write_page_metrics(&self.directory.join("page-usage.csv"), before, after)?;
        Self::write_page_usage_svg(&self.directory.join("page-usage.svg"), before, after)?;
        let traversal = Self::traversal(removers);
        log_progress(format_args!(
            "stage=instrumentation event=database_metrics body_pages={} body_used_pages={} average_body_page_usage={:.4} database_logical_bytes={} database_allocated_bytes={} average_nodes_followed_per_pop={:.3}",
            after.body.high_water_pages,
            after.body.used_pages,
            after.average_body_page_usage,
            after.total_file_bytes,
            storage_after.allocated_bytes,
            traversal.average_nodes_followed_per_pop()
        ));
        Ok(())
    }

    fn traversal(removers: &WorkerStats) -> BatchPopStats {
        BatchPopStats {
            pops: removers.inputs,
            nodes_followed: removers.nodes_followed,
        }
    }

    #[cfg(target_os = "linux")]
    fn write_mapping_manifest(path: &Path, database_path: &Path) -> AnyResult<()> {
        let database_path = std::fs::canonicalize(database_path)?;
        let database_text = path_text(&database_path, "database directory")?;
        let maps = std::fs::read_to_string("/proc/self/maps")?;
        let mut output = BufWriter::new(File::create(path)?);
        writeln!(output, "pid,start,end,file_offset,file")?;
        for line in maps.lines() {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let Some(mapped_path) = fields.get(5) else {
                continue;
            };
            if !mapped_path.starts_with(database_text) {
                continue;
            }
            let range = fields
                .first()
                .ok_or_else(|| io::Error::other("process mapping has no address range"))?;
            let (start, end) = range
                .split_once('-')
                .ok_or_else(|| io::Error::other("process mapping range is invalid"))?;
            let file_offset = fields
                .get(2)
                .ok_or_else(|| io::Error::other("process mapping has no file offset"))?;
            writeln!(
                output,
                "{},0x{start},0x{end},0x{file_offset},{}",
                std::process::id(),
                Self::csv_text(mapped_path)
            )?;
        }
        output.flush()?;
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn write_mapping_manifest(path: &Path, _database_path: &Path) -> AnyResult<()> {
        let mut output = BufWriter::new(File::create(path)?);
        writeln!(output, "pid,start,end,file_offset,file")?;
        output.flush()?;
        Ok(())
    }

    fn write_metrics_summary(
        path: &Path,
        before: &DatabaseStats,
        after: &DatabaseStats,
        storage_before: StorageStats,
        storage_after: StorageStats,
        removers: &WorkerStats,
    ) -> AnyResult<()> {
        let traversal = Self::traversal(removers);
        let mut output = BufWriter::new(File::create(path)?);
        writeln!(output, "metric,before_compaction,after_compaction")?;
        writeln!(
            output,
            "database_logical_bytes,{},{}",
            before.total_file_bytes, after.total_file_bytes
        )?;
        writeln!(
            output,
            "database_allocated_bytes,{},{}",
            storage_before.allocated_bytes, storage_after.allocated_bytes
        )?;
        writeln!(
            output,
            "heads_file_bytes,{},{}",
            before.heads_file_bytes, after.heads_file_bytes
        )?;
        Self::write_allocator_summary(&mut output, "body", &before.body, &after.body)?;
        if let (Some(before_blobs), Some(after_blobs)) = (&before.blobs, &after.blobs) {
            Self::write_allocator_summary(&mut output, "blobs", before_blobs, after_blobs)?;
        }
        writeln!(
            output,
            "average_body_page_usage,{:.8},{:.8}",
            before.average_body_page_usage, after.average_body_page_usage
        )?;
        writeln!(output, "pop_requests,{},{}", traversal.pops, traversal.pops)?;
        writeln!(
            output,
            "nodes_followed,{},{}",
            traversal.nodes_followed, traversal.nodes_followed
        )?;
        writeln!(
            output,
            "average_nodes_followed_per_pop,{0:.8},{0:.8}",
            traversal.average_nodes_followed_per_pop()
        )?;
        output.flush()?;
        Ok(())
    }

    fn write_allocator_summary(
        output: &mut impl Write,
        name: &str,
        before: &PageStats,
        after: &PageStats,
    ) -> io::Result<()> {
        writeln!(
            output,
            "{name}_page_size_bytes,{},{}",
            before.page_size_bytes, after.page_size_bytes
        )?;
        writeln!(
            output,
            "{name}_data_file_bytes,{},{}",
            before.data_file_bytes, after.data_file_bytes
        )?;
        writeln!(
            output,
            "{name}_count_file_bytes,{},{}",
            before.count_file_bytes, after.count_file_bytes
        )?;
        writeln!(
            output,
            "{name}_high_water_pages,{},{}",
            before.high_water_pages, after.high_water_pages
        )?;
        writeln!(
            output,
            "{name}_used_pages,{},{}",
            before.used_pages, after.used_pages
        )?;
        writeln!(
            output,
            "{name}_empty_pages,{},{}",
            before.empty_pages, after.empty_pages
        )?;
        writeln!(
            output,
            "{name}_claimed_pages,{},{}",
            before.claimed_pages, after.claimed_pages
        )?;
        writeln!(
            output,
            "{name}_claimed_but_unused_pages,{},{}",
            before.claimed_but_unused_pages, after.claimed_but_unused_pages
        )?;
        writeln!(
            output,
            "{name}_live_allocations,{},{}",
            before.live_allocations, after.live_allocations
        )?;
        writeln!(
            output,
            "{name}_average_page_load,{:.8},{:.8}",
            before.average_page_load, after.average_page_load
        )
    }

    fn write_page_metrics(
        path: &Path,
        before: &DatabaseStats,
        after: &DatabaseStats,
    ) -> AnyResult<()> {
        let mut output = BufWriter::new(File::create(path)?);
        writeln!(output, "phase,file,page,live_allocations")?;
        Self::write_page_metric_rows(&mut output, "before_compaction", "body", &before.body)?;
        if let Some(blobs) = &before.blobs {
            Self::write_page_metric_rows(&mut output, "before_compaction", "blobs", blobs)?;
        }
        Self::write_page_metric_rows(&mut output, "after_compaction", "body", &after.body)?;
        if let Some(blobs) = &after.blobs {
            Self::write_page_metric_rows(&mut output, "after_compaction", "blobs", blobs)?;
        }
        output.flush()?;
        Ok(())
    }

    fn write_page_metric_rows(
        output: &mut impl Write,
        phase: &str,
        file: &str,
        stats: &PageStats,
    ) -> io::Result<()> {
        for (page, live) in stats.page_loads.iter().copied().enumerate() {
            writeln!(output, "{phase},{file},{page},{live}")?;
        }
        Ok(())
    }

    fn write_page_usage_svg(
        path: &Path,
        before: &DatabaseStats,
        after: &DatabaseStats,
    ) -> AnyResult<()> {
        let mut panels = Vec::new();
        panels
            .try_reserve_exact(4)
            .map_err(|_| Error::OutOfMemory)?;
        panels.push((
            "body before compaction",
            &before.body,
            before.body_page_capacity,
        ));
        panels.push((
            "body after compaction",
            &after.body,
            after.body_page_capacity,
        ));
        if let Some(blobs) = &before.blobs {
            panels.push((
                "blobs before compaction",
                blobs,
                Self::maximum_page_load(blobs),
            ));
        }
        if let Some(blobs) = &after.blobs {
            panels.push((
                "blobs after compaction",
                blobs,
                Self::maximum_page_load(blobs),
            ));
        }

        let columns = 256_usize;
        let cell = 3_usize;
        let panel_heights = panels
            .iter()
            .map(|(_, stats, _)| stats.page_loads.len().div_ceil(columns).max(1) * cell + 52)
            .collect::<Vec<_>>();
        let height = panel_heights
            .iter()
            .try_fold(72_usize, |total, panel| total.checked_add(*panel))
            .ok_or_else(|| invalid_input("page heatmap height overflow"))?;
        let mut output = BufWriter::new(File::create(path)?);
        writeln!(
            output,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="{height}" viewBox="0 0 1080 {height}">"#
        )?;
        writeln!(
            output,
            r##"<rect width="1080" height="{height}" fill="#0f172a"/>"##
        )?;
        writeln!(
            output,
            r##"<text x="28" y="38" fill="#f8fafc" font-family="monospace" font-size="22">floresta-db allocator page usage</text>"##
        )?;
        let mut y = 64_usize;
        for ((label, stats, capacity), panel_height) in panels.iter().zip(panel_heights) {
            Self::write_heatmap_panel(&mut output, label, stats, *capacity, y, columns, cell)?;
            y = y
                .checked_add(panel_height)
                .ok_or_else(|| invalid_input("page heatmap position overflow"))?;
        }
        writeln!(output, "</svg>")?;
        output.flush()?;
        Ok(())
    }

    fn write_heatmap_panel(
        output: &mut impl Write,
        label: &str,
        stats: &PageStats,
        capacity: u64,
        y: usize,
        columns: usize,
        cell: usize,
    ) -> AnyResult<()> {
        writeln!(
            output,
            r##"<text x="28" y="{}" fill="#cbd5e1" font-family="monospace" font-size="15">{label}: pages={} used={} average_load={:.2}</text>"##,
            y + 16,
            stats.high_water_pages,
            stats.used_pages,
            stats.average_page_load
        )?;
        let grid_y = y + 28;
        for (page, live) in stats.page_loads.iter().copied().enumerate() {
            let x = 28 + page % columns * cell;
            let cell_y = grid_y + page / columns * cell;
            let color = Self::heat_color(u64::from(live), capacity);
            writeln!(
                output,
                r#"<rect x="{x}" y="{cell_y}" width="{cell}" height="{cell}" fill="{color}"><title>{label} page {page}: {live} live allocations</title></rect>"#
            )?;
        }
        Ok(())
    }

    fn maximum_page_load(stats: &PageStats) -> u64 {
        stats
            .page_loads
            .iter()
            .copied()
            .map(u64::from)
            .max()
            .unwrap_or(1)
            .max(1)
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn heat_color(load: u64, capacity: u64) -> String {
        if load == 0 || capacity == 0 {
            return "#1e293b".to_owned();
        }
        #[allow(clippy::cast_precision_loss)]
        let intensity = ((load as f64 / capacity as f64).clamp(0.0, 1.0).sqrt() * 255.0) as u8;
        let red = 30_u8.saturating_add(intensity.saturating_mul(3) / 4);
        let green = 64_u8.saturating_add(intensity / 3);
        let blue = 175_u8.saturating_sub(intensity / 2);
        format!("#{red:02x}{green:02x}{blue:02x}")
    }

    fn csv_text(value: &str) -> String {
        format!("\"{}\"", value.replace('"', "\"\""))
    }
}

/// Human-readable terminal summary for one complete run.
pub(super) struct RunReport<'stats> {
    pub(super) adders: &'stats WorkerStats,
    pub(super) removers: &'stats WorkerStats,
    pub(super) hints: &'stats HintsStats,
    pub(super) elapsed: Duration,
    pub(super) hints_elapsed: Duration,
    pub(super) storage: StorageStats,
}

impl RunReport<'_> {
    /// Prints stable key-value lines intended for operators and log ingestion.
    pub(super) fn print(&self) {
        println!(
            "status=ok elapsed_seconds={:.3} blocks_added={} blocks_removed={} eligible_outputs={} inputs_removed={} live_outputs={} block_mib_per_second={:.1}",
            self.elapsed.as_secs_f64(),
            self.adders.blocks,
            self.removers.blocks,
            self.hints.eligible_outputs,
            self.removers.inputs,
            self.hints.unspent_outputs,
            Self::mebibytes_per_second(
                self.adders.bytes.saturating_add(self.removers.bytes),
                self.elapsed
            ),
        );
        println!(
            "timing block_read_seconds={:.3} database_seconds={:.3} hints_seconds={:.3}",
            self.adders.block_read.as_secs_f64() + self.removers.block_read.as_secs_f64(),
            self.adders.database.as_secs_f64() + self.removers.database.as_secs_f64(),
            self.hints_elapsed.as_secs_f64(),
        );
        println!(
            "hints bytes={} unspent_outputs={}",
            self.hints.file_bytes, self.hints.unspent_outputs
        );
        println!(
            "storage files={} logical_gib={:.3} allocated_mib={:.1}",
            self.storage.files,
            Self::gibibytes_f64(self.storage.logical_bytes),
            Self::mebibytes_f64(self.storage.allocated_bytes),
        );
    }

    #[allow(clippy::cast_precision_loss)]
    fn mebibytes_per_second(bytes: u64, elapsed: Duration) -> f64 {
        Self::mebibytes_f64(bytes) / elapsed.as_secs_f64()
    }

    #[allow(clippy::cast_precision_loss)]
    fn mebibytes_f64(bytes: u64) -> f64 {
        bytes as f64 / (1024.0 * 1024.0)
    }

    #[allow(clippy::cast_precision_loss)]
    fn gibibytes_f64(bytes: u64) -> f64 {
        bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_database_instrumentation_artifacts() -> AnyResult<()> {
        let work_directory = std::env::temp_dir().join(format!(
            "floresta-db-hints-instrumentation-{}",
            std::process::id()
        ));
        let index_directory = work_directory.join("index");
        let _ignored = std::fs::remove_dir_all(&work_directory);
        std::fs::create_dir_all(&index_directory)?;
        let instrumentation = Instrumentation::create(&work_directory, &index_directory)?;
        let page_stats = |loads: Vec<u16>| PageStats {
            page_size_bytes: 64 * 1_024,
            data_file_bytes: 64 * 1_024 * 4,
            count_file_bytes: 64 * 1_024,
            high_water_pages: 3,
            used_pages: 2,
            empty_pages: 1,
            claimed_pages: 0,
            claimed_but_unused_pages: 0,
            live_allocations: 3,
            average_page_load: 1.5,
            page_loads: loads,
        };
        let before = DatabaseStats {
            heads_file_bytes: 64 * 1_024,
            body: page_stats(vec![2, 0, 1]),
            blobs: None,
            body_page_capacity: 2,
            average_body_page_usage: 0.75,
            total_file_bytes: 320 * 1_024,
        };
        let after = DatabaseStats {
            heads_file_bytes: before.heads_file_bytes,
            body: page_stats(vec![2, 1, 0]),
            blobs: None,
            body_page_capacity: 2,
            average_body_page_usage: 0.75,
            total_file_bytes: before.total_file_bytes,
        };
        let removers = WorkerStats {
            inputs: 2,
            nodes_followed: 5,
            ..WorkerStats::default()
        };
        let storage = StorageStats {
            files: 3,
            logical_bytes: before.total_file_bytes,
            allocated_bytes: 128 * 1_024,
        };
        instrumentation.write_database_snapshot(&before, &after, storage, storage, &removers)?;

        let directory = work_directory.join("instrumentation");
        let summary = std::fs::read_to_string(directory.join("summary.csv"))?;
        assert!(summary.contains("average_nodes_followed_per_pop,2.50000000,2.50000000"));
        let pages = std::fs::read_to_string(directory.join("page-usage.csv"))?;
        assert!(pages.contains("before_compaction,body,1,0"));
        let heatmap = std::fs::read_to_string(directory.join("page-usage.svg"))?;
        assert!(heatmap.contains("body after compaction page 1: 1 live allocations"));

        std::fs::remove_dir_all(work_directory)?;
        Ok(())
    }
}
