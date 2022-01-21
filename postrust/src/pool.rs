use crate::connection::Connection;
use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc::UnboundedSender, Mutex};

/// Asynchronous pool for generic pooled objects, including database connections
pub struct Pool<P = Connection>
where
    P: Pooled,
{
    /// Shared configuration for all Objects in the Pool
    configuration: P::Configuration,

    /// All pooled objects
    objects: Arc<Mutex<Vec<P>>>,

    /// Queue of objects to return to the Pool
    returning_objects: UnboundedSender<P>,
}

/// Implement shared methods over any poolable object
impl<P> Pool<P>
where
    P: Pooled + 'static,
    P::Configuration: fmt::Debug,
    P::Error: fmt::Display,
{
    /// Create a new Pool from a configuration
    #[tracing::instrument]
    pub fn new(configuration: P::Configuration) -> Result<Self, P::Error> {
        Self::new_with_objects(configuration, vec![])
    }

    /// Create a new pool from a configuration and set of objects
    // used internally for testing
    #[tracing::instrument(skip(objects))]
    pub(crate) fn new_with_objects(
        configuration: P::Configuration,
        objects: Vec<P>,
    ) -> Result<Self, P::Error> {
        let objects = Arc::new(Mutex::new(objects));

        // periodically clean up idle objects
        tokio::spawn({
            let objects = objects.clone();

            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let now = Instant::now();

                    let mut objects = objects.lock().await;

                    objects.retain(|object: &P| !object.should_drop(&now));
                }
            }
        });

        // periodically return objects to the pool
        let (returning_objects, mut returning_objects_receiver) =
            tokio::sync::mpsc::unbounded_channel();

        tokio::spawn({
            let objects = objects.clone();

            async move {
                while let Some(object) = returning_objects_receiver.recv().await {
                    let mut objects = objects.lock().await;

                    objects.push(object);

                    tracing::debug!(objects = objects.len(), "Object returned to the Pool");
                }
            }
        });

        tracing::info!("Pool initialized");

        Ok(Self {
            configuration,
            objects,
            returning_objects,
        })
    }

    /// Fetch an existing idle object from the pool (LIFO) or initialize a new one
    #[tracing::instrument]
    pub async fn get(&self) -> Result<PooledObject<P>, P::Error> {
        let mut objects = self.objects.lock().await;

        tracing::debug!(objects = objects.len(), "Fetching Object from the Pool");

        let object = objects.pop();

        drop(objects);

        let object = match object {
            Some(object) => object,
            None => P::create(&self.configuration).await?,
        };

        Ok(PooledObject {
            object: Some(object),
            return_sender: self.returning_objects.clone(),
        })
    }
}

impl<P> fmt::Debug for Pool<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("Pool")
            .field("configuration", &self.configuration)
            .finish()
    }
}

/// Cyclical collection of Pools that can be iterated endlessly
// Used for round-robin-style load balancing between endpoints
#[derive(Default)]
pub struct Pools<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    index: AtomicUsize,
    pools: Vec<Pool<P>>,
}

impl<P> fmt::Debug for Pools<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("Pools")
            .field("index", &self.index)
            .field("pools", &self.pools)
            .finish()
    }
}

impl<P> Pools<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    /// Create a new round-robin meta pool
    pub fn new(pools: Vec<Pool<P>>) -> Self {
        Self {
            index: AtomicUsize::new(0),
            pools,
        }
    }

    /// Iter-like next for selecting the next Pool in the Round Robin queue
    pub fn next(&self) -> Option<&Pool<P>> {
        if self.pools.is_empty() {
            None
        } else {
            let index = self.index.fetch_add(1, Ordering::Relaxed);

            self.pools.get(index % self.pools.len())
        }
    }

    /// Iter-like is_empty for checking if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }
}

/// Behavior for pooled objects
#[async_trait::async_trait]
pub trait Pooled: Sized + Send {
    type Configuration;
    type Error;

    /// Create a new poolable object from a configuration
    async fn create(configuration: &Self::Configuration) -> Result<Self, Self::Error>;

    /// If a creation error is recoverable, return how long to wait for the next retry
    fn should_retry_in(error: &Self::Error) -> Option<Duration>;

    /// Update hook for the pooled object, called on Drop
    fn update(&mut self) {
        // by default, do nothing
    }

    /// Handle for deciding if a pooled object should be dropped by the Pool at a certain time
    fn should_drop(&self, _drop_time: &Instant) -> bool {
        false // by default, do no clean up
    }
}

/// Wrapper around objects managed by a Pool for managing lifecycles
#[derive(Debug)]
pub struct PooledObject<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    object: Option<P>,
    return_sender: UnboundedSender<P>,
}

impl<P> Deref for PooledObject<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    type Target = P;

    fn deref(&self) -> &Self::Target {
        self.object.as_ref().unwrap()
    }
}

impl<P> DerefMut for PooledObject<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.as_mut().unwrap()
    }
}

impl<P> Drop for PooledObject<P>
where
    P: Pooled,
    P::Configuration: fmt::Debug,
{
    fn drop(&mut self) {
        if let Some(mut object) = self.object.take() {
            object.update();

            if let Err(error) = self.return_sender.send(object) {
                tracing::warn!(%error, "Error returning Object to Pool");
            }
        }
    }
}
