#![cfg_attr(not(test), no_std)]

extern crate alloc;

use core::future::ready;

use alloc::sync::Arc;
use derive_new::new;
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{self},
    signal::{self},
};
use embassy_time::{Duration, WithTimeout};
use futures::{StreamExt, TryStreamExt, stream};
use thiserror::Error;

pub type Channel<Request, Response> =
    channel::Channel<CriticalSectionRawMutex, Message<Request, Response>, 1>;
pub type Receiver<'r, Request, Response> =
    channel::Receiver<'r, CriticalSectionRawMutex, Message<Request, Response>, 1>;
pub type Sender<'r, Request, Response> =
    channel::Sender<'r, CriticalSectionRawMutex, Message<Request, Response>, 1>;

type Signal<R> = signal::Signal<CriticalSectionRawMutex, R>;

#[derive(Error, Debug, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Error {
    #[error("Timeout: {0}: {1}")]
    TimedOut(&'static str, Duration),
}

#[derive(Debug)]
pub struct CrossTaskConfig {
    /// for responders, a timeout between received requests.
    recv_timeout: Option<Duration>,

    /// for requestors, a timeout for sending a request into the queue
    send_timeout: Duration,

    /// for requestors, a timeout for the expected reply
    reply_timeout: Duration,
}

#[derive(new, Clone)]
pub struct Message<Request, Response> {
    request: Request,
    reply: Arc<Signal<Response>>,
}

#[derive(new)]
pub struct Requestor<Request: 'static, Response: 'static> {
    sender: Arc<Sender<'static, Request, Response>>,
    send_timeout: Duration,
    reply_timeout: Duration,
}

#[derive(new)]
pub struct Responder<Request: 'static, Response: 'static> {
    receiver: Receiver<'static, Request, Response>,
    recv_timeout: Option<Duration>,
}

impl Default for CrossTaskConfig {
    fn default() -> Self {
        Self {
            recv_timeout: None,
            send_timeout: Duration::from_millis(200),
            reply_timeout: Duration::from_millis(400),
        }
    }
}

impl<Request: Send, Response: Send> Requestor<Request, Response> {
    pub async fn request(&self, request: Request) -> Result<Response, Error> {
        let send_timed_out = |_| Error::TimedOut("send message", self.send_timeout);
        let reply_timed_out = |_| Error::TimedOut("message reply", self.reply_timeout);

        let signal = Arc::new(Signal::new());
        let message = Message::new(request, signal.clone());

        self.sender
            .send(message)
            .with_timeout(self.send_timeout)
            .await
            .map_err(send_timed_out)?;

        let response = signal
            .wait()
            .with_timeout(self.reply_timeout)
            .await
            .map_err(reply_timed_out)?;

        Ok(response)
    }
}

impl<Request, Respnse> Clone for Requestor<Request, Respnse> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            send_timeout: self.send_timeout,
            reply_timeout: self.reply_timeout,
        }
    }
}

impl<Request, Response> Responder<Request, Response> {
    async fn receive_next(&self) -> Result<Message<Request, Response>, Error> {
        let recv_timed_out = |_| {
            Error::TimedOut(
                "message receive",
                self.recv_timeout.unwrap_or_default(),
            )
        };

        if let Some(recv_timeout) = self.recv_timeout {
            self.receiver
                .receive()
                .with_timeout(recv_timeout)
                .await
                .map_err(recv_timed_out)
        } else {
            Ok(self.receiver.receive().await)
        }
    }

    pub async fn stream<Via>(&self, via: Via)
    where
        Via: AsyncFn(Request) -> Response,
        Via: Clone,
    {
        stream::unfold((), |_| async move {
            let next = self.receive_next().await;
            Some((next, ()))
        })
        .inspect_err(|e| {
            #[cfg(feature = "defmt")]
            defmt::warn!("receive error (ignoring): {:?}", e);
        })
        .filter_map(|x| ready(x.ok()))
        .for_each(|message| {
            let via = via.clone();
            let request = message.request;
            let reply = move |response| message.reply.signal(response);
            async move {
                let response = via(request).await;
                reply(response);
            }
        })
        .await;
    }
}

pub struct CrossTask<Request: 'static, Response: 'static> {
    pub requestor: Requestor<Request, Response>,
    pub responder: Responder<Request, Response>,
}

impl<Request: 'static, Response: 'static> CrossTask<Request, Response> {
    pub fn new(channel: &'static mut Channel<Request, Response>, config: CrossTaskConfig) -> Self {
        let sender = channel.sender();
        let receiver = channel.receiver();
        let responder = Responder::new(receiver, config.recv_timeout);
        let requestor = Requestor::new(Arc::new(sender), config.send_timeout, config.reply_timeout);
        Self {
            requestor,
            responder,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::ready;

    use crate::*;
    use static_cell::StaticCell;

    async fn _usage() {
        #[derive(Debug, PartialEq)]
        enum Request {
            Ping(u32),
        }

        #[derive(Debug, PartialEq)]
        enum Response {
            Pong(u32),
        }

        async fn ping(pinger: Requestor<Request, Response>) {
            let response = pinger.request(Request::Ping(1)).await;
            assert_eq!(Ok(Response::Pong(1)), response);
        }

        async fn pong(ponger: Responder<Request, Response>) {
            ponger
                .stream(|req| {
                    let response = match req {
                        Request::Ping(n) => Response::Pong(n),
                    };
                    ready(response)
                })
                .await;
        }

        static CHANNEL: StaticCell<Channel<Request, Response>> = StaticCell::new();
        let channel = CHANNEL.init(embassy_sync::channel::Channel::new());
        let CrossTask {
            requestor,
            responder,
        } = CrossTask::new(channel, CrossTaskConfig::default());

        let _ = tokio::select! {
            _ = ping(requestor) => (),
            _ = pong(responder) => ()
        };
    }
}
