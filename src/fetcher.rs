use std::cmp;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use futures::{future::FutureExt, select_biased};
use tbot::{
    types::parameters::{self, WebPagePreviewState},
    Bot,
};
use tokio::{
    self,
    stream::StreamExt,
    sync::Notify,
    time::{self, delay_for, delay_queue::DelayQueue, Duration, Instant},
};

use crate::client::pull_feed;
use crate::data::{
    SubscriberId,
    Database,
    Feed,
    FeedUpdate,
    FeedSettings,
};
use crate::messages::{format_large_msg, Escape};
use crate::feed;

pub fn start(bot: Bot, db: Arc<Mutex<Database>>, min_interval: u32, max_interval: u32) {
    let mut queue = FetchQueue::new();
    // TODO: Don't use interval, it can accumulate ticks
    // replace it with delay_until
    let mut interval = time::interval_at(Instant::now(), Duration::from_secs(min_interval as u64));
    let throttle = Throttle::new(min_interval as usize);
    tokio::spawn(async move {
        loop {
            select_biased! {
                feed = queue.next().fuse() => {
                    let feed = feed.expect("unreachable");
                    let bot = bot.clone();
                    let db = db.clone();
                    let opportunity = throttle.acquire();
                    tokio::spawn(async move {
                        opportunity.wait().await;
                        if let Err(e) = fetch_and_push_updates(bot, db, feed).await {
                            crate::print_error(e);
                        }
                    });
                }
                _ = interval.tick().fuse() => {
                    let feeds = db.lock().unwrap().all_feeds();
                    for feed in feeds {
                        let feed_interval = cmp::min(
                            cmp::max(
                                feed.ttl.map(|ttl| ttl * 60).unwrap_or_default(),
                                min_interval,
                            ),
                            max_interval,
                        ) as u64 - 1; // after -1, we can stagger with `interval`
                        queue.enqueue(feed, Duration::from_secs(feed_interval));
                    }
                }
            }
        }
    });
}

async fn fetch_and_push_updates(
    bot: Bot,
    db: Arc<Mutex<Database>>,
    feed: Feed,
) -> Result<(), tbot::errors::MethodCall> {
    let new_feed = match pull_feed(&feed.link).await {
        Ok(feed) => feed,
        Err(e) => {
            let down_time = db.lock().unwrap().get_or_update_down_time(&feed.link);
            if down_time.is_none() {
                // user unsubscribed while fetching the feed
                return Ok(());
            }
            // 5 days
            if down_time.unwrap().as_secs() > 5 * 24 * 60 * 60 {
                db.lock().unwrap().reset_down_time(&feed.link);
                let msg = format!(
                    "《<a href=\"{}\">{}</a>》\
                     已经连续 5 天拉取出错 ({}),\
                     可能已经关闭, 请取消订阅",
                    Escape(&feed.link),
                    Escape(&feed.title),
                    Escape(&e.to_user_friendly())
                );
                push_info_updates(&bot, &db, &feed, parameters::Text::html(&msg)).await?;
            }
            return Ok(());
        }
    };

    let updates = db.lock().unwrap().update(&feed.link, new_feed);
    for update in updates {
        match update {
            FeedUpdate::Items(items) => {
                push_rss_updates(
                    &bot,
                    &db,
                    &feed,
                    &items,
                )
                .await?;
            }
            FeedUpdate::Title(new_title) => {
                let msg = format!(
                    "<a href=\"{}\">{}</a> 已更名为 {}",
                    Escape(&feed.link),
                    Escape(&feed.title),
                    Escape(&new_title)
                );
                push_info_updates(
                    &bot,
                    &db,
                    &feed,
                    parameters::Text::html(&msg),
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn push_rss_updates(
    bot: &Bot,
    db: &Arc<Mutex<Database>>,
    feed: &Feed,
    items: &Vec<feed::Item>,
) -> Result<(), tbot::errors::MethodCall> {
    let msgs =
        format_large_msg(format!("<b>{}</b>", Escape(&feed.title)), &items, |item| {
            let title = item
                .title
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or_else(|| &feed.title);
            let link = item
                .link
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or_else(|| &feed.link);
            format!("<a href=\"{}\">{}</a>", Escape(link), Escape(title))
        });
    for msg in msgs {
        for subscriber in feed.subscribers.iter().copied() {
            let settings = db.lock().unwrap().get_setting(subscriber, &feed.link).unwrap();
            let formatted_msg = parameters::Text::html(&msg);
            push_message(&bot, &db, subscriber, &settings, formatted_msg).await?;
        }
    }
    Ok(())
}

async fn push_info_updates(
    bot: &Bot,
    db: &Arc<Mutex<Database>>,
    feed: &Feed,
    msg: parameters::Text<'_>,
) -> Result<(), tbot::errors::MethodCall> {
    for subscriber in feed.subscribers.iter().copied() {
        let settings = db.lock().unwrap().get_setting(subscriber, &feed.link).unwrap();
        push_message(&bot, &db, subscriber, &settings, msg).await?;
    }
    Ok(())
}

async fn push_message(
    bot: &Bot,
    db: &Arc<Mutex<Database>>,
    mut subscriber: SubscriberId,
    settings: &FeedSettings,
    msg: parameters::Text<'_>,
) -> Result<(), tbot::errors::MethodCall> {
    use tbot::errors::MethodCall;
    'retry: for _ in 0..3 {
        let mut bot_msg = bot.send_message(tbot::types::chat::Id(subscriber), msg);
        if settings.disable_preview.unwrap() {
            bot_msg = bot_msg.web_page_preview(WebPagePreviewState::Disabled)
        }
        match bot_msg
            .call()
            .await
        {
            Err(MethodCall::RequestError { description, .. })
                if chat_is_unavailable(&description) =>
            {
                db.lock().unwrap().delete_subscriber(subscriber);
            }
            Err(MethodCall::RequestError {
                migrate_to_chat_id: Some(new_chat_id),
                ..
            }) => {
                db.lock()
                    .unwrap()
                    .update_subscriber(subscriber, new_chat_id.0);
                subscriber = new_chat_id.0;
                continue 'retry;
            }
            Err(MethodCall::RequestError {
                retry_after: Some(delay),
                ..
            }) => {
                time::delay_for(Duration::from_secs(delay)).await;
                continue 'retry;
            }
            other => {
                other?;
            }
        }
        break 'retry;
    }
    Ok(())
}

pub fn chat_is_unavailable(s: &str) -> bool {
    s.contains("Forbidden")
        || s.contains("chat not found")
        || s.contains("have no rights")
        || s.contains("need administrator rights")
}

#[derive(Default)]
struct FetchQueue {
    feeds: HashMap<String, Feed>,
    notifies: DelayQueue<String>,
    wakeup: Notify,
}

impl FetchQueue {
    fn new() -> Self {
        Self::default()
    }

    fn enqueue(&mut self, feed: Feed, delay: Duration) -> bool {
        let exists = self.feeds.contains_key(&feed.link);
        if !exists {
            self.notifies.insert(feed.link.clone(), delay);
            self.feeds.insert(feed.link.clone(), feed);
            self.wakeup.notify();
        }
        !exists
    }

    async fn next(&mut self) -> Result<Feed, time::Error> {
        loop {
            if let Some(feed_id) = self.notifies.next().await {
                let feed = self.feeds.remove(feed_id?.get_ref()).unwrap();
                break Ok(feed);
            } else {
                self.wakeup.notified().await;
            }
        }
    }
}

struct Throttle {
    pieces: usize,
    counter: Arc<AtomicUsize>,
}

impl Throttle {
    fn new(pieces: usize) -> Self {
        Throttle {
            pieces,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn acquire(&self) -> Opportunity {
        Opportunity {
            n: self.counter.fetch_add(1, Ordering::AcqRel) % self.pieces,
            counter: self.counter.clone(),
        }
    }
}

#[must_use = "Don't lose your opportunity"]
struct Opportunity {
    n: usize,
    counter: Arc<AtomicUsize>,
}

impl Opportunity {
    async fn wait(&self) {
        delay_for(Duration::from_secs(self.n as u64)).await
    }
}

impl Drop for Opportunity {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}
