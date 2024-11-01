use super::{processor::Processor, task::Task};
use crate::replication::types::{DatabasePair, ReplicationStrategy};
use crate::TuxedoResult;
use futures_util::future::join_all;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub(crate) struct ReplicationConfig {
    pub(crate) thread_count: usize,
    pub(crate) batch_size: usize,
    pub(crate) strategy: ReplicationStrategy,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            strategy: ReplicationStrategy::Mask,
            thread_count: num_cpus::get(),
        }
    }
}

pub struct ReplicationManager {
    pub(crate) processors: Vec<Box<dyn Processor>>,
    pub(crate) task_receiver: mpsc::Receiver<Box<dyn Task>>,
    pub(crate) task_sender: mpsc::Sender<Box<dyn Task>>,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) config: ReplicationConfig,
    pub(crate) dbs: Arc<DatabasePair>,
}

impl ReplicationManager {
    pub async fn run(self) -> TuxedoResult<()> {
        let multi_progress = Arc::new(MultiProgress::new());
        let progress_style = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
        )
        .expect("Expected to set progress bar styling")
        .progress_chars("█▓▒░");

        // Spawn processor runners
        let processor_handles: Vec<_> = self
            .processors
            .into_iter()
            .map(|processor| {
                let dbs = Arc::clone(&self.dbs);
                let semaphore = Arc::clone(&self.semaphore);
                let task_sender = self.task_sender.clone();
                let default_config = self.config.clone();
                let progress_bar = multi_progress.add(ProgressBar::new(0));
                progress_bar.set_style(progress_style.clone());

                task::spawn(async move {
                    processor
                        .run(dbs, semaphore, task_sender, default_config, progress_bar)
                        .await;
                })
            })
            .collect();

        // Spawn ReplicationTask runners
        let runner_handle = task::spawn({
            let semaphore = Arc::clone(&self.semaphore);
            let mut task_receiver = self.task_receiver;
            async move {
                let mut join_set = JoinSet::new();

                loop {
                    tokio::select! {
                        Some(task) = task_receiver.recv() => {
                            let permit = semaphore.clone().acquire_owned().await.expect("Semaphore closed");
                            join_set.spawn(async move {
                                let _permit = permit;
                                task.run().await
                            });
                        }
                        else => break,
                    }

                    while join_set.len() >= self.config.thread_count {
                        if join_set
                            .join_next()
                            .await
                            .transpose()
                            .expect("Transpose failed")
                            .is_none()
                        {
                            break;
                        }
                    }
                }

                while let Some(result) = join_set.join_next().await {
                    result.expect("Join next failed");
                }
            }
        });

        // Wait for all the processors to finish generating tasks
        join_all(processor_handles)
            .await
            .into_iter()
            .collect::<Result<Vec<()>, _>>()?;

        // All tasks are completed, so we can drop the receiver to close the channel
        drop(self.task_sender);

        // Wait for the task runner to finish running all the tasks
        runner_handle.await.expect("Runner failed");

        Ok(())
    }
}
