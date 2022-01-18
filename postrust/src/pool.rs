use crate::{connection::Connection, endpoint::Endpoint, tcp};
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

// FIXME: make this configurable
/// Length of time that a Connection can be idle before getting cleaned up
const IDLE_CONNECTION_DURATION: Duration = Duration::from_secs(5);

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
    P: Pooled,
    P::Configuration: fmt::Debug,
    tcp::Error: From<P::Error>,
{
    /// Fetch an existing idle object from the pool (LIFO) or initialize a new one
    #[tracing::instrument(skip(self))] // FIXME: stop skipping self
    pub async fn get(&self) -> Result<PooledObject<P>, tcp::Error> {
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

/// Implement test-specific methods
#[cfg(test)]
impl<P> Pool<P>
where
    P: Pooled + 'static,
    P::Configuration: Default + fmt::Debug,
    tcp::Error: From<P::Error>,
{
    /// Handle creation of test Pool from a single test object
    pub fn test(object: P) -> Self {
        let (transmitter, mut reciever) = tokio::sync::mpsc::unbounded_channel();
        let objects = Arc::new(Mutex::new(vec![object]));
        let configuration = P::Configuration::default();

        tokio::spawn(async move {
            while let Some(_message) = reciever.recv().await {
                tracing::debug!("Test message recieved");
            }
        });

        Self {
            configuration,
            objects,
            returning_objects: transmitter,
        }
    }
}

/// Implement methods specifically for TCP-based database Connections
impl Pool<Connection> {
    /// Create a new Pool for an Endpoint
    #[tracing::instrument]
    pub async fn new(endpoint: Endpoint) -> Result<Self, tcp::Error> {
        let connections = Arc::new(Mutex::new(vec![]));

        // periodically clean up idle connections
        tokio::spawn({
            let connections = connections.clone();

            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let now = Instant::now();

                    let mut connections = connections.lock().await;

                    connections.retain(|connection: &Connection| {
                        now.duration_since(connection.last_used()) < IDLE_CONNECTION_DURATION
                    });
                }
            }
        });

        // periodically return connections to the pool
        let (returning_connections, mut returning_connections_receiver) =
            tokio::sync::mpsc::unbounded_channel();

        tokio::spawn({
            let connections = connections.clone();

            async move {
                while let Some(connection) = returning_connections_receiver.recv().await {
                    let mut connections = connections.lock().await;

                    connections.push(connection);

                    tracing::debug!(
                        connections = connections.len(),
                        "Connection returned to the Pool"
                    );
                }
            }
        });

        tracing::info!("Connection pool initialized");

        Ok(Self {
            configuration: endpoint,
            objects: connections,
            returning_objects: returning_connections,
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

    /// Update hook for the pooled object, called on Drop
    fn update(&mut self) {}
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
                tracing::warn!(%error, "Error returning Connection to Pool");
            }
        }
    }
}
