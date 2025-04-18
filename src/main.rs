/// A Discord bot that deletes messages after a certain amount of time or when a maximum number of messages is reached.
///
/// This is inspired by <https://github.com/riking/AutoDelete>
/// Its maintainer is on extended hiatus, so here's a rewrite in Rust.
///
/// Some improvements over the original:
/// * Uses slash commands instead of regular prefix commands
///
/// TODO:
/// * Maybe an admin HTTP server with some status info
use anyhow::{anyhow, Context as AnyhowContext};
use async_recursion::async_recursion;
use chrono::{TimeDelta, Utc};
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use poise::{
    say_reply,
    serenity_prelude::{
        CacheHttp, ChannelId, ChannelType, ClientBuilder, FullEvent, GatewayIntents, GetMessages,
        GuildChannel, Http, Message, MessageId, PartialGuildChannel, Ready, StatusCode, Timestamp,
    },
    FrameworkContext,
};
use pretty_duration::pretty_duration;
use rusqlite::{params, Connection};
use std::{
    collections::{HashMap, VecDeque},
    env,
    fmt::{Display, Formatter},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
#[cfg(unix)]
use tokio::signal::unix::SignalKind;
use tokio::{select, sync::Notify};

mod schema;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

const DB_PATH: &str = "autodelete.db";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Before,
    After,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Before => write!(f, "before"),
            Direction::After => write!(f, "after"),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
enum TaskStatus {
    #[default]
    Starting,
    Running,
    Stopping,
    Stopped,
}

#[derive(Debug, Default)]
struct ChannelInner {
    max_age: Option<TimeDelta>,
    max_messages: Option<usize>,
    message_ids: VecDeque<MessageId>,
    delete_queue: Vec<MessageId>,
    last_seen_message: Option<MessageId>,
    bot_start_message: Option<MessageId>,
    delete_task_status: TaskStatus,
    expire_task_status: TaskStatus,
    fetched_history: bool,
}

impl ChannelInner {
    fn check_expiry(&mut self) -> (usize, Option<TimeDelta>) {
        let mut expired = 0;
        let mut duration_to_next = None;
        if let Some(max_age) = self.max_age {
            let now = Utc::now();
            let oldest_allowed = Timestamp::from(now - max_age);
            while let Some(first) = self.message_ids.front() {
                if first.created_at() < oldest_allowed {
                    debug!(
                        "Expiring message {}(msg_timestamp={} expire_time={})",
                        first,
                        first.created_at(),
                        oldest_allowed
                    );
                    self.delete_queue
                        .push(self.message_ids.pop_front().unwrap());
                    expired += 1;
                } else {
                    // Deref gives the TimeDelta, which implements Add & Sub
                    let to_next = *first.created_at() + max_age - now;
                    // It has to be in the future, because it's not expired yet
                    assert!(to_next > TimeDelta::zero());
                    duration_to_next = Some(to_next);
                    // messages should be sorted by timestamp, so we can stop here.
                    break;
                }
            }
        }
        (expired, duration_to_next)
    }

    fn check_max_messages(&mut self) -> usize {
        let mut moved_to_delete_queue = 0;
        if let Some(max_messages) = self.max_messages {
            while self.message_ids.len() > max_messages {
                self.delete_queue
                    .push(self.message_ids.pop_front().unwrap());
                // If there's a max_age, the expire task might wake up
                // too early now, but the way to update it is to wake it up.
                // So leave as is for now.
                moved_to_delete_queue += 1;
            }
        }
        moved_to_delete_queue
    }
}

#[derive(Debug, Default)]
struct ChannelOuter {
    // ID in outer so we don't have to lock the whole thing to read
    channel_id: ChannelId,
    // Categories are actually parents to channels, but we only use this for
    // threads.
    parent_id: Option<ChannelId>,
    inner: Mutex<ChannelInner>,
    delete_notify: Notify,
    expire_notify: Notify,
}

#[derive(Clone, Debug, Default)]
struct Channel(Arc<ChannelOuter>);

impl Channel {
    fn new(
        http: Arc<Http>,
        channel_id: ChannelId,
        parent_id: Option<ChannelId>,
        inner: ChannelInner,
        runtime: &Arc<tokio::runtime::Runtime>,
    ) -> Self {
        let channel = Channel(Arc::new(ChannelOuter {
            channel_id,
            parent_id,
            inner: Mutex::new(inner),
            delete_notify: Notify::new(),
            expire_notify: Notify::new(),
        }));
        channel.clone().expire_task(runtime);
        channel.clone().delete_task(http, runtime);
        channel
    }

    // Background task to delete messages
    // This is intentionally separate from the delete queue, to try and get as
    // few actual delete calls as possible, using bulk deletes.
    fn delete_task(self, http: Arc<Http>, runtime: &Arc<tokio::runtime::Runtime>) {
        runtime.spawn(async move {
            let channel_id = self.0.channel_id;
            debug!("Starting delete task for channel {}", channel_id);
            {
                let mut channel_guard = self.0.inner.lock().unwrap();
                assert_eq!(channel_guard.delete_task_status, TaskStatus::Starting);
                channel_guard.delete_task_status = TaskStatus::Running;
            }
            loop {
                // Wait at the top of the loop so we can use continue and still wait
                self.0.delete_notify.notified().await;
                debug!("Delete task for channel {} notified", channel_id);
                let mut delete_queue_local = Vec::new();
                let delete_thread = {
                    let mut channel_guard = self.0.inner.lock().unwrap();
                    if channel_guard.delete_task_status == TaskStatus::Stopping {
                        break;
                    }
                    delete_queue_local.append(&mut channel_guard.delete_queue);
                    self.0.parent_id.is_some()
                        && channel_guard.fetched_history
                        && channel_guard.message_ids.is_empty()
                };
                // If the thread is empty, just delete the thread.
                if delete_thread {
                    let result = channel_id.delete(&http).await;
                    if result.is_err() {
                        error!("Error deleting thread: {:?}", result);
                        // If we fail to delete, at least do the message deletions
                    } else {
                        continue;
                    }
                }
                // Normally we shouldn't have an empty local queue, but it's possible
                // because we're sleeping on Notify. But check if empty to save some
                // work.
                if delete_queue_local.is_empty() {
                    continue;
                }
                // Messages older than two weeks can't be bulk deleted, so split
                // the queue into two parts
                let two_weeks_ago = Timestamp::from(
                    // Take one hour off to avoid race conditions with clocks that are
                    // a little (up to an hour) out of sync.
                    Utc::now() - TimeDelta::weeks(2) + TimeDelta::hours(1),
                );
                let total = delete_queue_local.len();
                let mut progress: usize = 0;
                let (delete_queue_non_bulk, delete_queue_bulk) = delete_queue_local
                    .into_iter()
                    .partition::<Vec<_>, _>(|x| x.created_at() <= two_weeks_ago);
                for chunk in delete_queue_bulk.chunks(100) {
                    {
                        let mut channel_guard = self.0.inner.lock().unwrap();
                        if channel_guard.delete_task_status == TaskStatus::Stopping {
                            channel_guard.delete_queue.extend(chunk);
                            // not break, because we want further chunks returned as well
                            continue;
                        }
                    }
                    progress += chunk.len();
                    if chunk.len() == 1 {
                        delete_message_from_discord(
                            &http, channel_id, chunk[0], &self, &progress, &total,
                        )
                        .await;
                    } else {
                        debug!(
                            "Deleting {} messages in bulk (max per request is 100) [{}/{}]",
                            chunk.len(),
                            progress,
                            total
                        );
                        let result = channel_id.delete_messages(&http, chunk.iter()).await;
                        if result.is_err() {
                            error!("Error deleting messages: {:?}", result);
                            debug!("Putting messages back into delete queue");
                            self.0.inner.lock().unwrap().delete_queue.extend(chunk);
                            self.0.delete_notify.notify_one();
                        }
                    }
                }
                if !delete_queue_non_bulk.is_empty() {
                    warn!(
                        "Deleting {} messages that are older than two weeks",
                        delete_queue_non_bulk.len()
                    );
                    for message_id in delete_queue_non_bulk {
                        // This is a lock per message, but each delete_message might stall on
                        // rate limiting for quite a while. So we want to check between each delete.
                        {
                            let mut channel_guard = self.0.inner.lock().unwrap();
                            if channel_guard.delete_task_status == TaskStatus::Stopping {
                                channel_guard.delete_queue.push(message_id);
                                // not break, because we want further messages returned as well
                                continue;
                            }
                        }
                        delete_message_from_discord(
                            &http, channel_id, message_id, &self, &progress, &total,
                        )
                        .await;
                    }
                }
                let channel_guard = self.0.inner.lock().unwrap();
                if channel_guard.delete_task_status == TaskStatus::Stopping {
                    break;
                }
            }
            debug!("Delete task for channel {} stopped", channel_id);
            let mut channel_guard = self.0.inner.lock().unwrap();
            channel_guard.delete_task_status = TaskStatus::Stopped;
        });
    }

    // Background task to put expired messages in the delete queue.
    //
    // This needs to go on a separate runtime because of reasons in exit_handler
    fn expire_task(self, runtime: &Arc<tokio::runtime::Runtime>) {
        runtime.spawn(async move {
            let channel_id = self.0.channel_id;
            debug!("Starting expire task for channel {}", channel_id);
            {
                let mut channel_guard = self.0.inner.lock().unwrap();
                assert_eq!(channel_guard.expire_task_status, TaskStatus::Starting);
                channel_guard.expire_task_status = TaskStatus::Running;
            }
            loop {
                let sleep_time = {
                    let mut channel_guard = self.0.inner.lock().unwrap();
                    if channel_guard.expire_task_status == TaskStatus::Stopping {
                        debug!("Stopping expire task for channel {}", channel_id);
                        break;
                    }
                    let (expired, sleep_time) = channel_guard.check_expiry();
                    if expired > 0 || !channel_guard.delete_queue.is_empty() {
                        debug!("{} messages expired in channel {}", expired, channel_id);
                        self.0.delete_notify.notify_one();
                    }
                    sleep_time
                };
                // Sleep at least 1 second, so that deletes can possibly be grouped
                let sleep_time = sleep_time.map_or(TimeDelta::MAX, |x| {
                    if x < TimeDelta::seconds(1) {
                        TimeDelta::seconds(1)
                    } else {
                        x
                    }
                });
                debug!(
                    "Expire task sleeping for {} for channel {}",
                    if sleep_time == TimeDelta::MAX {
                        "forever".to_string()
                    } else {
                        format!("{sleep_time:?} seconds")
                    },
                    channel_id
                );
                select! {
                    () = tokio::time::sleep(sleep_time.to_std().unwrap()) => {
                        debug!("Expire task for channel {channel_id} woke up");
                    }
                    () = self.0.expire_notify.notified() => {
                        debug!("Expire task for channel {channel_id} notified");
                    }
                };
            }
            debug!("Expire task for channel {} stopped", channel_id);
            let mut channel_guard = self.0.inner.lock().unwrap();
            channel_guard.expire_task_status = TaskStatus::Stopped;
        });
    }

    fn stop_tasks(&self) {
        let mut channel_guard = self.0.inner.lock().unwrap();
        channel_guard.delete_task_status = TaskStatus::Stopping;
        channel_guard.expire_task_status = TaskStatus::Stopping;
        drop(channel_guard);
        self.0.delete_notify.notify_one();
        self.0.expire_notify.notify_one();
    }

    fn tasks_stopped(&self) -> bool {
        let channel_guard = self.0.inner.lock().unwrap();
        channel_guard.delete_task_status == TaskStatus::Stopped
            && channel_guard.expire_task_status == TaskStatus::Stopped
    }
}

#[derive(Clone, Debug, Default)]
struct Channels(Arc<Mutex<HashMap<ChannelId, Channel>>>);

impl Channels {
    fn get_or_default(
        &self,
        http: Arc<Http>,
        channel_id: ChannelId,
        parent_id: Option<ChannelId>,
        runtime: &Arc<tokio::runtime::Runtime>,
    ) -> Channel {
        // Since this is only called when changing a channel's setting now,
        // we can just lock with `write()`, and not worry about race conditions
        // between the `get()` and `insert()`.
        let mut channels = self.0.lock().unwrap();
        if let Some(channel) = channels.get(&channel_id).cloned() {
            channel
        } else {
            let channel = Channel::new(http, channel_id, parent_id, ChannelInner::default(), runtime);
            channels.insert(channel_id, channel.clone());
            channel
        }
    }

    fn get(&self, channel_id: ChannelId) -> Option<Channel> {
        self.0.lock().unwrap().get(&channel_id).cloned()
    }

    fn remove(&self, channel_id: ChannelId) -> Option<Channel> {
        let channel = self.0.lock().unwrap().remove(&channel_id);
        if let Some(ref channel) = channel {
            channel.stop_tasks();
        }
        channel
    }

    fn to_cloned_vec(&self) -> Vec<(ChannelId, Channel)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(channel_id, channel)| (*channel_id, channel.clone()))
            .collect()
    }
}

type DbUpdaterTx = mpsc::Sender<ChannelId>;
type DbUpdaterRx = mpsc::Receiver<ChannelId>;

struct Data {
    background_task_runtime: Arc<tokio::runtime::Runtime>,
    // User data, which is stored and accessible in all command invocations
    channels: Channels,
    db_updater: DbUpdaterTx,
}

async fn fetch_archived_threads(
    http: Arc<Http>,
    channel_id: ChannelId,
    private: bool,
) -> Result<Vec<GuildChannel>, Error> {
    let mut before = None;
    let mut threads = Vec::new();
    loop {
        let threadsdata = if private {
            channel_id
                .get_archived_private_threads(&http, before, None)
                .await?
        } else {
            channel_id
                .get_archived_public_threads(&http, before, None)
                .await?
        };
        debug!("Got {} archived threads", threadsdata.threads.len());
        before = threadsdata.threads.last().map(|t| {
            u64::try_from(
                t.thread_metadata
                    .unwrap()
                    .archive_timestamp
                    .unwrap()
                    .unix_timestamp(),
            )
            .unwrap()
        });
        threads.extend(threadsdata.threads);
        if !threadsdata.has_more {
            break;
        }
    }
    Ok(threads)
}

#[async_recursion]
async fn fetch_threads(
    data: &Data,
    http: Arc<Http>,
    channel_id: ChannelId,
    max_age: Option<TimeDelta>,
    max_messages: Option<usize>,
) -> Result<(), Error> {
    let guild_id = channel_id
        .to_channel(&http)
        .await?
        .guild()
        .unwrap()
        .guild_id;

    let threadsdata = guild_id.get_active_threads(&http).await?;
    if threadsdata.has_more {
        warn!("More threads than Discord returned, some might be missing. This shouldn't happen, please inform the developer at https://github.com/docwilco/rusty-trash-can/issues");
    }
    let active_threads = threadsdata.threads;
    let archived_private_threads = fetch_archived_threads(http.clone(), channel_id, true).await?;
    let archived_public_threads = fetch_archived_threads(http.clone(), channel_id, false).await?;

    debug!(
        "Got {} active, {} private archived, {} public archived threads",
        active_threads.len(),
        archived_private_threads.len(),
        archived_public_threads.len()
    );
    let threads = active_threads
        .into_iter()
        .chain(archived_private_threads)
        .chain(archived_public_threads);
    for thread in threads {
        debug!("Checking thread {}", thread.id);
        if thread.parent_id == Some(channel_id) {
            debug!("Thread {} is a child of {}", thread.id, channel_id);
            add_or_update_channel(
                http.clone(),
                data,
                thread.id,
                thread.parent_id,
                max_age,
                max_messages,
                None,
            )
            .await?;
        }
    }
    Ok(())
}

async fn fetch_message_history(
    db_updater: DbUpdaterTx,
    http: &impl CacheHttp,
    channel: Channel,
    direction: Direction,
    message_id: Option<MessageId>,
) -> Result<(), Error> {
    let channel_id = channel.0.channel_id;
    let bot_start_message = channel.0.inner.lock().unwrap().bot_start_message;
    info!(
        "Fetching message history for channel {}. Requesting messages {} {}",
        channel_id,
        direction,
        message_id.unwrap_or_default()
    );
    let mut origin = message_id;
    // If there's no message_id, we're fetching the most recent messages
    // so always go backwards in that case.
    let direction = if message_id.is_none() {
        Direction::Before
    } else {
        direction
    };
    loop {
        // These requests are automatically rate limited by serenity, so it
        // can take a while to fetch a lot of messages. Put them in our channel
        // metadata as we get each chunk.
        let builder = GetMessages::new();
        let builder = match (origin, direction) {
            (Some(origin), Direction::Before) => builder.before(origin),
            (Some(origin), Direction::After) => builder.after(origin),
            (None, _) => builder,
        }
        .limit(100);
        let messages = channel_id.messages(http, builder).await?;
        if messages.is_empty() {
            break;
        }
        for message in &messages {
            trace!("Fetched message {}, kind: {:?}", message.id, message.kind);
        }
        if direction == Direction::After {
            origin = Some(messages.first().unwrap().id);
        } else {
            origin = Some(messages.last().unwrap().id);
        }
        let mut message_ids: Vec<MessageId> = messages
            .into_iter()
            .filter_map(|m| {
                // Filter out the bot start message
                if let Some(bot_start_message) = bot_start_message {
                    if m.id == bot_start_message {
                        return None;
                    }
                }
                Some(m.id)
            })
            .collect();
        info!(
            "Fetched {} messages for channel {}",
            message_ids.len(),
            channel_id
        );

        // Add to the channel's data
        // The messages are currently going from newest to oldest, but we want
        // them in the opposite order, because that's how the channel data has
        // them.
        message_ids.reverse();
        // Convert to a VecDeque so we can use `append()` for speed
        let mut ids = VecDeque::from(message_ids);
        let mut channel_guard = channel.0.inner.lock().unwrap();
        // Combine with existing message ids, but remove duplicates that might
        // have been added by the message event handler while we were
        // retrieving history.
        match direction {
            Direction::Before => {
                // We're getting history from before what we know, so put
                // the new messages at the front. Do that by appending what
                // we might have to the just retrieved messages.
                // There used to be code here, but it turned out After does
                // everything Before does, just one thing extra, so nothing
                // inside the match remains.
            }
            Direction::After => {
                // We're getting history from after what we know, so put
                // the new messages at the end. Do that by appending to the
                // channel metadata, and then deduping the whole thing.
                // A benchmark showed that this is faster than using the code
                // from the Before branch, despite the extra `append()`.
                channel_guard.message_ids.append(&mut ids);
            }
        }
        ids.append(&mut channel_guard.message_ids);
        ids.make_contiguous().sort_unstable();
        channel_guard.message_ids.extend(ids.into_iter().dedup());
        channel_guard.check_max_messages();
        if !channel_guard.delete_queue.is_empty() {
            channel.0.delete_notify.notify_one();
        }
        drop(channel_guard);
        // Let the expire task know there's new messages for
        // every chunk so expiry & deletion can get started
        // while we're fetching.
        channel.0.expire_notify.notify_one();
    }
    db_updater.send(channel_id).unwrap();
    Ok(())
}

#[poise::command(slash_command, subcommands("start", "stop", "status"))]
/// The bot's main entry point.
///
/// Doesn't do anything since we're slash commands only now, and slash commands
/// can't do toplevel if there's subs.
///
// Commands must be async, but this is empty.
#[allow(clippy::unused_async)]
async fn autodelete(_ctx: Context<'_>) -> Result<(), Error> {
    Ok(())
}

fn status_message(max_age: Option<TimeDelta>, max_messages: Option<usize>) -> String {
    let max_age = max_age.map(|x| pretty_duration(&x.to_std().unwrap(), None));
    match (max_age, max_messages) {
        (Some(max_age), Some(max_messages)) => {
            format!("max age: {max_age}, max messages: {max_messages}")
        }
        (None, Some(max_messages)) => format!("max messages: {max_messages}"),
        (Some(max_age), None) => format!("max age: {max_age}"),
        (None, None) => "Autodelete is not active".to_string(),
    }
}

/// Set or update autodelete settings for the current channel
///
/// This command has 2 arguments:
///
/// `max_age`: The maximum age of messages to keep. Use a number followed by a
/// unit (`s` or `second`, `m` or `minute`, `h` or `hour`, `d` or `day`).
/// `max_messages`: The maximum number of messages to keep.
///
/// Both are optional, but at least one is required.
///
/// Note that using a `max_age` of 14 days (`14d`) or higher is not recommended,
/// as Discord does not allow bulk deleting messages older than 14 days. It will
/// still work, but it might be slower due to Discord's rate limits.
///
/// Likewise, using a `max_messages` with a high number, without `max_age` for a
/// busy channel can also run into rate limits.
///
/// This command requires the `MANAGE_MESSAGES` permission.
#[poise::command(slash_command, required_permissions = "MANAGE_MESSAGES")]
async fn start(
    ctx: Context<'_>,
    #[description = "Max age of messages"] max_age: Option<String>,
    #[description = "Max number of messages to keep"] max_messages: Option<usize>,
) -> Result<(), Error> {
    let channel_id = ctx.channel_id();
    // Should not be PrivateChannel or ChannelCategory
    let channel = channel_id.to_channel(ctx).await?.guild().unwrap();
    match channel.kind {
        ChannelType::PublicThread | ChannelType::PrivateThread | ChannelType::NewsThread => {
            return Err("Autodelete in threads is controlled through the parent channel, please use this command there".into());
        }
        ChannelType::Text => {}
        _ => {
            return Err("Autodelete is not supported in this channel type".into());
        }
    }
    let max_age = max_age.map(duration_str::parse_chrono).transpose();
    if max_age.is_err() {
        return Err("Invalid max age, use a number followed by a unit (`s` or `second`, `m` or `minute`, `h` or `hour`, `d` or `day`)".into());
    }
    let max_age = max_age.unwrap();
    if max_age.is_none() && max_messages.is_none() {
        return Err("Must specify at least one of max messages or max age".into());
    }
    let message = status_message(max_age, max_messages);
    info!("Updating settings for {}: {}", channel_id, message);
    let message = format!("Updating autodelete settings: {message}");
    let reply = say_reply(ctx, message).await?;
    let reply_id = reply.message().await?.id;
    let http = ctx.serenity_context().http.clone();
    add_or_update_channel(
        http,
        ctx.data(),
        channel_id,
        None,
        max_age,
        max_messages,
        Some(reply_id),
    )
    .await
    .unwrap();
    Ok(())
}

async fn add_or_update_channel(
    http: Arc<Http>,
    data: &Data,
    channel_id: ChannelId,
    parent_id: Option<ChannelId>,
    max_age: Option<TimeDelta>,
    max_messages: Option<usize>,
    bot_start_message: Option<MessageId>,
) -> Result<(), Error> {
    debug!("Adding or updating channel {}", channel_id);
    let channel = data
        .channels
        .get_or_default(http.clone(), channel_id, parent_id, &data.background_task_runtime);
    let was_inactive = {
        let mut channel_guard = channel.0.inner.lock().unwrap();
        let was_inactive = channel_guard.max_age.is_none() && channel_guard.max_messages.is_none();
        channel_guard.max_age = max_age;
        channel_guard.max_messages = max_messages;
        channel_guard.bot_start_message = bot_start_message;
        was_inactive
    };
    data.db_updater.send(channel_id).unwrap();

    if was_inactive {
        info!(
            "Channel {} wasn't autodeleting, fetching message history",
            channel_id
        );
        fetch_message_history(
            data.db_updater.clone(),
            &http.clone(),
            channel,
            Direction::Before,
            // We might have a race on our hands in a busy channel, so just
            // don't try to guess where we're at. We need to get a lot of
            // history anyways, since this is a new channel for us.
            None,
        )
        .await?;
        if parent_id.is_none() {
            fetch_threads(data, http, channel_id, max_age, max_messages).await?;
        }
    } else {
        // Since the max_age might have changed, wake up the expire task
        // so it can recalculate the sleep time.
        // Since fetch_message_history() notifies the tasks, the other
        // branch of this if is covered.
        channel.0.expire_notify.notify_one();
    }
    Ok(())
}

#[poise::command(slash_command, required_permissions = "MANAGE_MESSAGES")]
/// Stop autodelete for the current channel
///
/// Avoid stopping just to start with different settings, as all the message
/// history will be deleted when stopping. Meaning that it will have to be
/// retrieved again when setting up autodelete again.
///
/// Of course, this could be desirable, if the bot has lost track of things.
async fn stop(ctx: Context<'_>) -> Result<(), Error> {
    let message = if remove_channel(ctx.data(), ctx.channel_id()) {
        "Autodelete stopped"
    } else {
        "Autodelete was not active"
    };
    say_reply(ctx, message).await?;
    Ok(())
}

fn remove_channel(data: &Data, channel_id: ChannelId) -> bool {
    // Remove any threads we might have
    for thread_id in data
        .channels
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(id, channel)| {
            if channel.0.parent_id == Some(channel_id) {
                Some(*id)
            } else {
                None
            }
        })
    {
        remove_channel(data, thread_id);
    }

    let return_value = data.channels.remove(channel_id).is_some();
    // Remove from database
    data.db_updater.send(channel_id).unwrap();
    return_value
}

#[poise::command(slash_command, required_permissions = "MANAGE_MESSAGES")]
/// Show autodelete settings for the current channel
async fn status(ctx: Context<'_>) -> Result<(), Error> {
    let channel = ctx.data().channels.get(ctx.channel_id());
    let message = match channel {
        Some(channel) => {
            let channel_guard = channel.0.inner.lock().unwrap();
            status_message(channel_guard.max_age, channel_guard.max_messages)
        }
        None => "Autodelete is not active".to_string(),
    };
    debug!("Status for {}: {}", ctx.channel_id(), message);
    say_reply(ctx, message).await?;
    Ok(())
}

async fn delete_message_from_discord(
    http: &Http,
    channel_id: ChannelId,
    message_id: MessageId,
    channel: &Channel,
    progress: &usize,
    total: &usize,
) {
    debug!("Deleting message {} [{}/{}]", message_id, progress, total);
    let delete = channel_id.delete_message(http, message_id).await;
    match delete {
        Err(serenity::Error::Http(e)) if e.status_code() == Some(StatusCode::NOT_FOUND) => {
            // Message was already deleted
            warn!("404: Message {} was already deleted", message_id);
        }
        Err(serenity::Error::Http(e)) if e.status_code() == Some(StatusCode::FORBIDDEN) => {
            // We don't have permission to delete the message
            warn!("403: No permission to delete message {}", message_id);
        }
        Err(e) => {
            error!("Error deleting message: {:?}", e);
            debug!("Returning message to queue");
            channel
                .0
                .inner
                .lock()
                .unwrap()
                .delete_queue
                .push(message_id);
            channel.0.delete_notify.notify_one();
        }
        Ok(()) => {}
    }
}

// Thread to save the message IDs periodically
//
// This works on all channels in one sweep, to minimize the number of database
// calls. And also to have the disk writes be grouped together.
//
// Also responsible for updating single channels when prompted through the
// channel
fn db_updater_thread(rx: DbUpdaterRx, channels: Channels) {
    thread::spawn(move || {
        // This is just to be able to use ? in the closure
        move || -> Result<(), Error> {
            // Use a separate connection for the save task
            debug!("Started save thread");
            let mut db_connection = Connection::open(DB_PATH)?;
            // Get sleep time from environment, defaults to 5 minutes
            let interval = std::env::var("SAVE_INTERVAL")
                .map(|x| x.parse::<u64>().expect("SAVE_INTERVAL must be a number"))
                .unwrap_or(300);
            let interval = Duration::from_secs(interval);
            let mut next_save = Instant::now() + interval;
            loop {
                let mut now = Instant::now();
                if now < next_save {
                    // Sleep until it's time to save again or we get something
                    // on the channel.
                    match rx.recv_timeout(next_save - now) {
                        Ok(channel_id) => {
                            let channel = channels.get(channel_id);
                            if let Some(channel) = channel {
                                save_channel_to_db(&mut db_connection, &channel)?;
                            } else {
                                delete_channel_from_db(&mut db_connection, channel_id)?;
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            break;
                        }
                    }
                }
                // We have either slept or saved something to the database, so
                // now has definitely changed.
                now = Instant::now();
                if now >= next_save {
                    next_save += interval;
                    save_all_message_ids_to_db(&mut db_connection, &channels.to_cloned_vec())?;
                }
            }
            Ok(())
        }()
        .unwrap();
    });
}

// SQLite doesn't support unsigned integers, so we have to cast them to signed and back
#[allow(clippy::cast_possible_wrap)]
fn save_channel_to_db(db_connection: &mut Connection, channel: &Channel) -> Result<(), Error> {
    let channel_id = channel.0.channel_id;
    debug!("Saving settings & message IDs for {}", channel_id);
    let channel_guard = channel.0.inner.lock().unwrap();
    let channel_id = channel_id.get() as i64;
    let parent_id = channel.0.parent_id.map(|x| x.get() as i64);
    let max_age = channel_guard
        .max_age
        .map(|x| x.to_std().unwrap().as_secs() as i64);
    let max_messages = channel_guard.max_messages.map(|x| x as i64);
    let last_seen_message = channel_guard.last_seen_message.map(|x| x.get() as i64);
    let bot_start_message = channel_guard.bot_start_message.map(|x| x.get() as i64);
    let message_ids = channel_guard.message_ids.clone();
    drop(channel_guard);
    let insert_or_replace = "INSERT OR REPLACE INTO channel_settings (
            channel_id,
            parent_id,
            max_age,
            max_messages,
            last_seen_message,
            bot_start_message)
        VALUES (?, ?, ?, ?, ?, ?)";
    let params = params![
        channel_id,
        parent_id,
        max_age,
        max_messages,
        last_seen_message,
        bot_start_message
    ];
    let tx = db_connection.transaction()?;
    tx.execute(insert_or_replace, params)?;
    tx.execute("DELETE FROM message_ids WHERE channel_id = ?", [channel_id])?;
    let mut insert_id =
        tx.prepare("INSERT INTO message_ids (channel_id, message_id) VALUES (?, ?)")?;
    for message_id in message_ids {
        insert_id.execute([channel_id, message_id.get() as i64])?;
    }
    drop(insert_id);
    tx.commit()?;
    debug!("Channel {} done saving", channel_id);
    Ok(())
}

// SQLite doesn't support unsigned integers, so we have to cast them to signed and back
#[allow(clippy::cast_possible_wrap)]
fn delete_channel_from_db(
    db_connection: &mut Connection,
    channel_id: ChannelId,
) -> Result<(), Error> {
    debug!("Deleting settings & message IDs for {}", channel_id);
    let tx = db_connection.transaction()?;
    tx.execute(
        "DELETE FROM message_ids WHERE channel_id = ?",
        [channel_id.get() as i64],
    )?;
    tx.execute(
        "DELETE FROM channel_settings WHERE channel_id = ?",
        [channel_id.get() as i64],
    )?;
    tx.commit()?;
    debug!("Channel {} done deleting", channel_id);
    Ok(())
}

// SQLite doesn't support unsigned integers, so we have to cast them to signed and back
#[allow(clippy::cast_possible_wrap)]
// This doesn't use `save_message_ids` so that the DELETE is a single statement
// without WHERE clause, which sqlite can optimize. Also that way it's a single
// transaction for the whole thing.
fn save_all_message_ids_to_db(
    db_connection: &mut Connection,
    channels_local: &[(ChannelId, Channel)],
) -> Result<(), Error> {
    // First we build up an in memory list of all data we want to save, so that
    // we don't have to wait on locks while writing to the database.
    let mut all_ids = Vec::new();
    let mut all_last_seens = Vec::new();
    // Using for loop because async closures aren't stable yet
    for (channel_id, channel) in channels_local {
        let channel_guard = channel.0.inner.lock().unwrap();
        for message_id in &channel_guard.message_ids {
            all_ids.push((*channel_id, *message_id));
        }
        all_last_seens.push((*channel_id, channel_guard.last_seen_message));
    }

    let tx = db_connection.transaction()?;
    // Delete instead of truncate, so the transaction can be rolled
    // back. Actually, sqlite just doesn't have truncate.
    tx.execute("DELETE FROM message_ids", [])?;
    let mut insert_id =
        tx.prepare("INSERT INTO message_ids (channel_id, message_id) VALUES (?, ?)")?;
    let mut update_last_seen =
        tx.prepare("UPDATE channel_settings SET last_seen_message = ? WHERE channel_id = ?")?;
    for (channel_id, message_id) in &all_ids {
        insert_id.execute([channel_id.get() as i64, message_id.get() as i64])?;
    }
    for (channel_id, last_seen) in &all_last_seens {
        update_last_seen.execute(params![
            last_seen.map(|id| id.get() as i64),
            channel_id.get() as i64,
        ])?;
    }
    drop(insert_id);
    drop(update_last_seen);
    tx.commit()?;
    Ok(())
}

fn ready_handler(http: &Arc<Http>, data: &Data, data_about_bot: &Ready) {
    info!("{} is connected!", data_about_bot.user.name);

    // Catch up on messages that have happened since the bot was last online
    let channels = data.channels.to_cloned_vec();
    for (_, channel) in channels {
        let last_seen = channel.0.inner.lock().unwrap().last_seen_message;
        let http = http.clone();
        let db_updater = data.db_updater.clone();
        tokio::spawn(async move {
            fetch_message_history(db_updater, &http, channel, Direction::After, last_seen)
                .await
                .unwrap();
        });
    }
}

fn message_handler(data: &Data, new_message: &Message) {
    debug!(
        "Received message {} on channel {}",
        new_message.id, new_message.channel_id
    );
    let channel = data.channels.get(new_message.channel_id);
    let Some(channel) = channel else {
        debug!("Ignoring message in channel without settings");
        return;
    };
    let mut channel_guard = channel.0.inner.lock().unwrap();
    if channel_guard.max_age.is_none() && channel_guard.max_messages.is_none() {
        error!("Ignoring message in channel without both maxes (shouldn't happen?)");
        return;
    }
    if let Some(bot_start_message) = channel_guard.bot_start_message {
        if bot_start_message == new_message.id {
            debug!("Ignoring bot start message");
            return;
        }
    }
    channel_guard.last_seen_message = Some(new_message.id);
    let was_empty = channel_guard.message_ids.is_empty();
    channel_guard.message_ids.push_back(new_message.id);
    channel_guard.check_max_messages();
    if !channel_guard.delete_queue.is_empty() {
        channel.0.delete_notify.notify_one();
    }
    drop(channel_guard);
    if was_empty {
        // If the message queue was empty, it would be waiting for
        // MAX duration, so notify the task to wake up.
        channel.0.expire_notify.notify_one();
    }
}

async fn thread_create_handler(
    http: Arc<Http>,
    data: &Data,
    thread: &GuildChannel,
) -> Result<(), Error> {
    let parent_id = thread
        .parent_id
        .expect("Threads should always have parents");
    debug!(
        "Received thread create {} on channel {}",
        thread.id, parent_id
    );
    let channel = data.channels.get(parent_id);
    let Some(channel) = channel else {
        debug!("Ignoring thread create in channel without settings");
        return Ok(());
    };
    let (max_age, max_messages) = {
        let channel_guard = channel.0.inner.lock().unwrap();
        (channel_guard.max_age, channel_guard.max_messages)
    };
    add_or_update_channel(
        http,
        data,
        thread.id,
        Some(parent_id),
        max_age,
        max_messages,
        None,
    )
    .await
}

fn thread_delete_handler(data: &Data, thread: &PartialGuildChannel) {
    debug!("Received thread delete {}", thread.id);
    remove_channel(data, thread.id);
}

async fn event_event_handler(
    framework: FrameworkContext<'_, Data, Error>,
    event: &FullEvent,
) -> Result<(), Error> {
    let data = framework.user_data;
    let ctx = framework.serenity_context;

    trace!("Event: {:#?}", event);
    match event {
        FullEvent::Ready { data_about_bot } => {
            ready_handler(&ctx.http, data, data_about_bot);
        }
        FullEvent::Message { new_message } => {
            message_handler(data, new_message);
        }
        FullEvent::ThreadCreate { thread } => {
            thread_create_handler(ctx.http.clone(), data, thread).await?;
        }
        FullEvent::ThreadDelete {
            thread,
            full_thread_data: _,
        } => {
            thread_delete_handler(data, thread);
        }
        _ => (),
    }

    Ok(())
}

#[cfg(unix)]
async fn wait_for_termination() {
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            debug!("Received Ctrl+C");
        }
        _ = sigterm.recv() => {
            debug!("Received SIGTERM");
        }
    }
}

#[cfg(windows)]
async fn wait_for_termination() {
    let mut ctrl_break = tokio::signal::windows::ctrl_break().unwrap();
    let mut ctrl_c = tokio::signal::windows::ctrl_c().unwrap();
    let mut ctrl_close = tokio::signal::windows::ctrl_close().unwrap();
    let mut ctrl_logoff = tokio::signal::windows::ctrl_logoff().unwrap();
    let mut ctrl_shutdown = tokio::signal::windows::ctrl_shutdown().unwrap();
    tokio::select! {
        _ = ctrl_break.recv() => {
            debug!("Received Ctrl+Break");
        }
        _ = ctrl_c.recv() => {
            debug!("Received Ctrl+C");
        }
        _ = ctrl_close.recv() => {
            debug!("Received Ctrl+Close");
        }
        _ = ctrl_logoff.recv() => {
            debug!("Received Ctrl+Logoff");
        }
        _ = ctrl_shutdown.recv() => {
            debug!("Received Ctrl+Shutdown");
        }
    }
}

fn exit_handler(all_channels: Channels) {
    // Because the things we want to do on exit are not async, we spawn a thread
    // to do them, since they have to wait for async tasks to finish while
    // holding the lock on the channels map, and we don't want to block the
    // async runtime.
    //
    // So make a oneshot and make the thread the receiver. This allows the
    // runtime to run the expire/deletion tasks that we're waiting on to finish.
    // And those tasks can run because we're only holding the lock on the map,
    // not on the inner channel data.
    //
    // However... incoming events can still try to access the channels map, so
    // that can block the runtime. Which in turn can cause the background tasks
    // to never finish. So we need to either:
    // - Make the event handler aware that we're shutting down before we lock
    //   the map. For instance using an AtomicBool.
    // - Put the background tasks on a separate tokio runtime, so they can
    //   always finish up.
    //
    // I _think_ the separate runtime is faster, because there's less atomic
    // operations that way.
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        wait_for_termination().await;
        info!("Signal received, shutting down.");
        // Send the signal to the thread
        tx.send(()).unwrap();
    });

    thread::spawn(move || {
        let mut db_connection = Connection::open(DB_PATH).unwrap();
        // Wait for the signal to exit
        rx.recv().unwrap();
        // We hang on to the lock until the end of the function, so that nothing
        // can be added, nor that the periodic save task can run before we exit.
        let all_channels_guard = all_channels.0.lock().unwrap();
        let all_channels_local = all_channels_guard
            .iter()
            .map(|(id, channel)| (*id, channel.clone()))
            .collect::<Vec<_>>();
        // Stop the tasks on the channels
        debug!("Stopping tasks");
        for (_, channel) in &all_channels_local {
            channel.stop_tasks();
        }
        // Wait for all the channels' tasks to finish
        debug!("Waiting for tasks to finish...");
        for (_, channel) in &all_channels_local {
            loop {
                if channel.tasks_stopped() {
                    break;
                }
                debug!("Waiting for tasks to finish (100ms sleep)");
                thread::sleep(Duration::from_millis(100));
            }
        }
        debug!("Tasks finished");
        // Return messages from delete queues to message IDs
        debug!("Returning messages from delete queues");
        for (channel_id, channel) in &all_channels_local {
            let mut channel_guard = channel.0.inner.lock().unwrap();
            debug!(
                "Channel {} has {} messages in delete queue",
                channel_id,
                channel_guard.delete_queue.len()
            );
            while let Some(message_id) = channel_guard.delete_queue.pop() {
                channel_guard.message_ids.push_back(message_id);
            }
        }
        save_all_message_ids_to_db(&mut db_connection, &all_channels_local).unwrap();
        warn!("Saved message IDs, exiting due to signal.");
        std::process::exit(0);
    });
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
// SQLite doesn't support unsigned integers, so we have to cast them to signed and back
#[allow(clippy::cast_sign_loss)]
async fn main() -> Result<(), Error> {
    dotenvy::dotenv().ok();
    env_logger::init();
    info!("Starting up");

    let mut conn = Connection::open(DB_PATH)?;
    schema::check_and_upgrade(&mut conn)?;

    let mut channels_query = conn.prepare(
        "SELECT channel_id,
            parent_id,
            max_age,
            max_messages,
            last_seen_message,
            bot_start_message
        FROM channel_settings",
    )?;
    let channels: Vec<(ChannelId, Option<ChannelId>, ChannelInner)> = channels_query
        .query_map([], |row| {
            let channel_id: i64 = row.get(0)?;
            let parent_id: Option<i64> = row.get(1)?;
            let max_age: Option<i64> = row.get(2)?;
            let max_messages: Option<i64> = row.get(3)?;
            let last_seen_message: Option<i64> = row.get(4)?;
            let bot_start_message: Option<i64> = row.get(5)?;
            Ok((
                ChannelId::new(channel_id as u64),
                parent_id.map(|id| ChannelId::new(id as u64)),
                ChannelInner {
                    max_age: max_age.map(TimeDelta::seconds),
                    max_messages: max_messages.map(|x| usize::try_from(x).unwrap()),
                    last_seen_message: last_seen_message.map(|id| MessageId::from(id as u64)),
                    bot_start_message: bot_start_message.map(|id| MessageId::from(id as u64)),
                    ..Default::default()
                },
            ))
        })?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;

    info!("Loaded channel settings from database");
    debug!("Channel settings:");
    for (id, parent_id, inner) in &channels {
        if let Some(parent) = parent_id {
            debug!("{id} (parent: {parent}): {inner:?}");
        } else {
            debug!("{id}: {inner:?}");
        }
    }

    let mut message_ids_query = conn.prepare(
        "SELECT channel_id, message_id
            FROM message_ids
            ORDER BY channel_id, message_id",
    )?;
    let mut message_ids: HashMap<ChannelId, VecDeque<MessageId>> = HashMap::new();
    for row in message_ids_query.query_map([], |row| {
        let channel_id: i64 = row.get(0)?;
        let message_id: i64 = row.get(1)?;
        Ok((
            ChannelId::new(channel_id as u64),
            MessageId::new(message_id as u64),
        ))
    })? {
        let (channel_id, message_id) = row?;
        message_ids
            .entry(channel_id)
            .or_default()
            .push_back(message_id);
    }
    info!("Loaded message IDs from database");
    debug!("Loaded message IDs: {:?}", message_ids);

    drop(message_ids_query);
    drop(channels_query);
    drop(conn);

    let (db_updater_tx, db_updater_rx) = mpsc::channel();
    // Setup's closure requires things to be Sync, so wrap a mutex around the
    // receiver. Just to `into_inner()` it inside the closure.
    let db_updater_rx = Mutex::new(db_updater_rx);

    let token = env::var("DISCORD_TOKEN").expect("missing DISCORD_TOKEN");
    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::GUILDS;
    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![autodelete()],
            event_handler: |framework, event| Box::pin(event_event_handler(framework, event)),
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;

                let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
                // Converting from ChannelInner to Channel requires the Http client, so we do it here
                let channels = channels
                    .into_iter()
                    .map(|(channel_id, parent_id, mut inner)| {
                        // `remove` so we can just move it out of the HashMap
                        inner.message_ids = message_ids.remove(&channel_id).unwrap_or_default();
                        (
                            channel_id,
                            Channel::new(ctx.http.clone(), channel_id, parent_id, inner, &runtime),
                        )
                    })
                    .collect::<HashMap<ChannelId, Channel>>();
                let channels = Channels(Arc::new(Mutex::new(channels)));
                // Now that we're inside the closure, we can into_inner this to
                // get rid of the Mutex
                let db_updater_rx = db_updater_rx.into_inner().unwrap();
                // background thread for writing to database
                db_updater_thread(db_updater_rx, channels.clone());
                // background task to handle exiting
                exit_handler(channels.clone());
                let data = Data {
                    background_task_runtime: runtime,
                    channels,
                    db_updater: db_updater_tx,
                };
                Ok(data)
            })
        })
        .build();
    let mut client = ClientBuilder::new(token, intents)
        .framework(framework)
        .await
        .context(anyhow!("Client build failed"))?;
    info!("Initialization complete. Starting framework!");
    client.start().await.context("Client start failed")?;
    Ok(())
}
