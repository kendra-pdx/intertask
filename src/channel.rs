#[cfg(feature = "embassy")]
mod embassy;

#[cfg(feature = "std")]
mod std;

use alloc::boxed::Box;
use async_trait::async_trait;

pub trait Shared: Send + Sync + 'static {}

#[async_trait]
pub trait Receiver: Shared + Clone {
    type Item: Shared;
    async fn recv(&self) -> Self::Item;
}

#[async_trait]
pub trait Sender: Shared + Clone {
    type Item: Shared;
    async fn send(&self, item: Self::Item);
}

#[async_trait]
pub trait Channel {
    type Item: Shared;
    type Receiver: Receiver<Item = Self::Item>;
    type Sender: Sender<Item = Self::Item>;

    fn new() -> Self;
    fn receiver(&self) -> Self::Receiver;
    fn sender(&self) -> Self::Sender;
}
