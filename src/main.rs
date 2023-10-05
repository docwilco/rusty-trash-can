/// A Discord bot that deletes messages after a certain amount of time or when a maximum number of messages is reached.
///
/// This is inspired by https://github.com/riking/AutoDelete
/// Its maintainer is on extended hiatus, so here's a rewrite in Rust.
///
/// Some improvements over the original:
/// * Uses slash commands in addition to regular commands
///
/// TODO:
/// * Maybe an admin HTTP server with some status info
use itertools::Itertools;
use log::{debug, error, info, warn};
use poise::say_reply;
use poise::serenity_prelude::{
    ChannelId, GatewayIntents, Http, MessageId, Mutex, RwLock, StatusCode, Timestamp,
};
use rusqlite::params;
use rusqlite::Error::SqliteFailure;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::{Duration, OffsetDateTime};
use tokio::select;
#[cfg(unix)]
use tokio::signal::unix::SignalKind;
use tokio::sync::Notify;
use tokio_rusqlite::Connection;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Message {
    id: MessageId,
    attachments: bool,
    embeds: bool,
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)?;
        if self.attachments {
            write!(f, " (attachments)")?;
        }
        if self.embeds {
            write!(f, " (embeds)")?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ChannelInner {
    max_age: Option<Duration>,
    max_messages: Option<u64>,
    attachments_max_age: Option<Duration>,
    attachments_max_messages: Option<u64>,
    embeds_max_age: Option<Duration>,
    embeds_max_messages: Option<u64>,
    messages: VecDeque<Message>,
    delete_queue: Vec<Message>,
    last_seen_message: Option<MessageId>,
    bot_start_message: Option<MessageId>,
    stop_tasks: bool,
    delete_task_stopped: bool,
    expire_task_stopped: bool,
}

impl ChannelInner {
    fn check_stopped(&mut self, channel_id: ChannelId) -> bool {
        if self.stop_tasks {
            debug!("Stopping delete task for channel {}", channel_id);
            true
        } else {
            false
        }
    }

    fn check_expiry(&mut self) -> (usize, Option<StdDuration>) {
        let mut expired = 0;
        let mut duration_to_next = None;
        if let Some(max_age) = self.max_age {
            let now = OffsetDateTime::now_utc();
            let oldest_allowed = Timestamp::from(now - max_age);
            while let Some(first) = self.messages.front() {
                if first.id.created_at() < oldest_allowed {
                    debug!(
                        "Expiring message {}(msg_timestamp={} expire_time={})",
                        first.id,
                        first.id.created_at(),
                        oldest_allowed
                    );
                    self.delete_queue.push(self.messages.pop_front().unwrap());
                    expired += 1;
                } else {
                    // Deref gives the OffsetDateTime, which implements Add & Sub
                    let to_next = *first.id.created_at() + max_age - now;
                    // It has to be in the future, because it's not expired yet
                    assert!(to_next > Duration::ZERO);
                    duration_to_next = Some(to_next.unsigned_abs());
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
            while self.messages.len() > max_messages as usize {
                self.delete_queue.push(self.messages.pop_front().unwrap());
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
    inner: Mutex<ChannelInner>,
    delete_notify: Notify,
    expire_notify: Notify,
}

#[derive(Clone, Debug, Default)]
struct Channel(Arc<ChannelOuter>);

impl Channel {
    fn new(http: Arc<Http>, channel_id: ChannelId, inner: ChannelInner) -> Self {
        let channel = Channel(Arc::new(ChannelOuter {
            channel_id,
            inner: Mutex::new(inner),
            delete_notify: Notify::new(),
            expire_notify: Notify::new(),
        }));
        channel.clone().expire_task();
        channel.clone().delete_task(http);
        channel
    }

    // Background task to delete messages
    // This is intentionally separate from the delete queue, to try and get as
    // few actual delete calls as possible, using bulk deletes.
    fn delete_task(self, http: Arc<Http>) {
        tokio::spawn(async move {
            let channel_id = self.0.channel_id;
            debug!("Starting delete task for channel {}", channel_id);
            let mut stopping = false;
            loop {
                // Wait at the top of the loop so we can use continue and still wait
                self.0.delete_notify.notified().await;
                debug!("Delete task for channel {} notified", channel_id);
                let mut delete_queue_local = Vec::new();
                let mut channel_guard = self.0.inner.lock().await;
                if channel_guard.check_stopped(channel_id) {
                    channel_guard.delete_task_stopped = true;
                    break;
                }
                delete_queue_local.append(&mut channel_guard.delete_queue);
                drop(channel_guard);
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
                    OffsetDateTime::now_utc() - Duration::weeks(2) + Duration::hours(1),
                );
                let total = delete_queue_local.len();
                let mut progress: usize = 0;
                let (delete_queue_non_bulk, delete_queue_bulk) = delete_queue_local
                    .into_iter()
                    .partition::<Vec<_>, _>(|x| x.id.created_at() <= two_weeks_ago);
                for chunk in delete_queue_bulk.chunks(100) {
                    let mut channel_guard = self.0.inner.lock().await;
                    if stopping || channel_guard.check_stopped(channel_id) {
                        stopping = true;
                        channel_guard.delete_queue.extend(chunk.into_iter().cloned());
                        // not break, because we want further chunks returned as well
                        continue;
                    }
                    drop(channel_guard);
                    progress += chunk.len();
                    match chunk.len() {
                        1 => {
                            delete_message(&http, channel_id, chunk[0], &self, &progress, &total)
                                .await;
                        }
                        _ => {
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
                                self.0.inner.lock().await.delete_queue.extend(chunk);
                                self.0.delete_notify.notify_one();
                            }
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
                        let mut channel_guard = self.0.inner.lock().await;
                        if channel_guard.check_stopped(channel_id) {
                            channel_guard.delete_queue.push(message_id);
                            // not break, because we want further messages returned as well
                            continue;
                        }
                        drop(channel_guard);
                        delete_message(&http, channel_id, message_id, &self, &progress, &total)
                            .await;
                    }
                }
                let mut channel_guard = self.0.inner.lock().await;
                if channel_guard.check_stopped(channel_id) {
                    channel_guard.delete_task_stopped = true;
                    break;
                }
            }
        });
    }

    // background task to put expired messages in the delete queue
    fn expire_task(self) {
        tokio::spawn(async move {
            let channel_id = self.0.channel_id;
            debug!("Starting expire task for channel {}", channel_id);
            loop {
                let mut channel_guard = self.0.inner.lock().await;
                if channel_guard.stop_tasks {
                    debug!("Stopping expire task for channel {}", channel_id);
                    channel_guard.expire_task_stopped = true;
                    break;
                }
                let (expired, sleep_time) = channel_guard.check_expiry();
                if expired > 0 || !channel_guard.delete_queue.is_empty() {
                    debug!("{} messages expired in channel {}", expired, channel_id);
                    self.0.delete_notify.notify_one();
                }
                drop(channel_guard);
                // Sleep at least 1 second, so that deletes can possibly be grouped
                let sleep_time = sleep_time
                    .map(|x| {
                        if x < StdDuration::from_secs(1) {
                            StdDuration::from_secs(1)
                        } else {
                            x
                        }
                    })
                    .unwrap_or(StdDuration::MAX);
                debug!(
                    "Expire task sleeping for {} for channel {}",
                    if sleep_time == StdDuration::MAX {
                        "forever".to_string()
                    } else {
                        format!("{:?} seconds", sleep_time)
                    },
                    channel_id
                );
                select! {
                    _ = tokio::time::sleep(sleep_time) => {
                        debug!("Expire task for channel {} woke up", channel_id);
                    }
                    _ = self.0.expire_notify.notified() => {
                        debug!("Expire task for channel {} notified", channel_id);
                    }
                };
            }
        });
    }
}

#[derive(Clone, Debug, Default)]
struct Channels(Arc<RwLock<HashMap<ChannelId, Channel>>>);

impl Channels {
    async fn get_or_default(&self, http: Arc<Http>, channel_id: ChannelId) -> Channel {
        // Since this is only called when changing a channel's setting now,
        // we can just lock with `write()`, and not worry about race conditions
        // between the `get()` and `insert()`.
        let mut channels = self.0.write().await;
        match channels.get(&channel_id).cloned() {
            Some(channel) => channel,
            None => {
                let channel = Channel::new(http, channel_id, ChannelInner::default());
                channels.insert(channel_id, channel.clone());
                channel
            }
        }
    }

    async fn get(&self, channel_id: ChannelId) -> Option<Channel> {
        self.0.read().await.get(&channel_id).cloned()
    }

    async fn remove(&self, channel_id: ChannelId) -> Option<Channel> {
        let channel = self.0.write().await.remove(&channel_id);
        if let Some(ref channel) = channel {
            channel.0.inner.lock().await.stop_tasks = true;
            channel.0.delete_notify.notify_one();
            channel.0.expire_notify.notify_one();
        }
        channel
    }

    async fn to_cloned_vec(&self) -> Vec<(ChannelId, Channel)> {
        self.0
            .read()
            .await
            .iter()
            .map(|(channel_id, channel)| (*channel_id, channel.clone()))
            .collect()
    }
}

struct Data {
    // User data, which is stored and accessible in all command invocations
    channels: Channels,
    db_connection: Arc<Mutex<Connection>>,
}

async fn fetch_message_history(
    db_connection: Arc<Mutex<Connection>>,
    http: impl AsRef<Http>,
    channel: Channel,
    direction: Direction,
    message_id: Option<MessageId>,
) -> Result<(), Error> {
    let channel_id = channel.0.channel_id;
    let bot_start_message = channel.0.inner.lock().await.bot_start_message;
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
        // These requests are automatically rate limited by serenity (and
        // Discord), so it can take a while to fetch a lot of messages. Put them
        // in our channel metadata as we get each chunk.
        let messages = channel_id
            .messages(&http, |b| match (origin, direction) {
                (Some(origin), Direction::After) => b.after(origin).limit(100),
                (Some(origin), Direction::Before) => b.before(origin).limit(100),
                (None, _) => b.limit(100),
            })
            .await?;
        if messages.is_empty() {
            break;
        }
        if direction == Direction::After {
            origin = Some(messages.first().unwrap().id);
        } else {
            origin = Some(messages.last().unwrap().id);
        }
        let mut messages: Vec<Message> = messages
            .into_iter()
            .filter_map(|m| {
                // Filter out the bot start message
                if let Some(bot_start_message) = bot_start_message {
                    if m.id == bot_start_message {
                        return None;
                    }
                }
                Some(Message {
                    id: m.id,
                    attachments: !m.attachments.is_empty(),
                    embeds: !m.embeds.is_empty(),
                })
            })
            .collect();
        info!(
            "Fetched {} messages for channel {}",
            messages.len(),
            channel_id
        );

        // Log to database
        log_message_ids(db_connection.clone(), channel_id, &messages)
            .await
            .expect("Error logging to database");
        // Add to the channel's data
        // The messages are currently going from newest to oldest, but we want
        // them in the opposite order, because that's how the channel data has
        // them.
        messages.reverse();
        // Convert to a VecDeque so we can use `append()` for speed
        let mut ids = VecDeque::from(messages);
        let mut channel_guard = channel.0.inner.lock().await;
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
                channel_guard.messages.append(&mut ids);
            }
        }
        ids.append(&mut channel_guard.messages);
        ids.make_contiguous().sort_unstable();
        channel_guard.messages.extend(ids.into_iter().dedup());
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
    save_message_ids(db_connection, channel).await;
    Ok(())
}

/// Show help for the autodelete command
#[poise::command(slash_command)]
async fn help(
    ctx: Context<'_>,
    #[description = "Specific command to show help about"] mut command: Option<String>,
) -> Result<(), Error> {
    // This makes it possible to just make `help` a subcommand of any command
    // `/autodelete help` turns into `/help autodelete`
    // `/autodelete help start` turns into `/help autodelete start`
    if ctx.invoked_command_name() != "help" {
        command = match command {
            Some(c) => Some(format!("{} {}", ctx.invoked_command_name(), c)),
            None => Some(ctx.invoked_command_name().to_string()),
        };
    }
    let config = poise::builtins::HelpConfiguration {
        show_subcommands: true,
        ..Default::default()
    };
    poise::builtins::help(ctx, command.as_deref(), config).await?;
    Ok(())
}

#[poise::command(slash_command, subcommands("start", "stop", "status", "help"))]
/// The bot's main entry point.
///
/// Doesn't do anything since we're slash commands only now, and slash commands
/// can't do toplevel if there's subs.
async fn autodelete(_ctx: Context<'_>) -> Result<(), Error> {
    Ok(())
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
    #[description = "Max number of messages to keep"] max_messages: Option<u64>,
) -> Result<(), Error> {
    let max_age = max_age.map(duration_str::parse_time).transpose();
    if max_age.is_err() {
        return Err("Invalid max age, use a number followed by a unit (`s` or `second`, `m` or `minute`, `h` or `hour`, `d` or `day`)".into());
    }
    let max_age = max_age.unwrap();
    if max_age.is_none() && max_messages.is_none() {
        return Err("Must specify at least one of max messages or max age".into());
    }
    let http = ctx.serenity_context().http.clone();
    let channel = ctx
        .data()
        .channels
        .get_or_default(http, ctx.channel_id())
        .await;
    let mut channel_guard = channel.0.inner.lock().await;
    let was_inactive = channel_guard.max_age.is_none() && channel_guard.max_messages.is_none();
    channel_guard.max_age = max_age;
    channel_guard.max_messages = max_messages;
    channel_guard.last_seen_message = Some(ctx.id().into());
    // Keep the lock until we have our reply id, otherwise the reply
    // might be trigger a message event before we know to ignore it.

    // Write to database
    let channel_id = ctx.channel_id();
    let last_seen_message = channel_guard.last_seen_message;
    let bot_start_message = channel_guard.bot_start_message;
    ctx.data()
        .db_connection
        .lock()
        .await
        .call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO channel_settings
                (channel_id, max_age, max_messages, last_seen_message, bot_start_message)
                VALUES (?, ?, ?, ?, ?)",
                params![
                    channel_id.0 as i64,
                    max_age.map(|x| x.whole_seconds()),
                    max_messages.map(|x| x as i64),
                    last_seen_message.map(|x| x.0 as i64),
                    bot_start_message.map(|x| x.0 as i64),
                ],
            )?;
            debug!("Updated channel settings for {} in database", channel_id);
            Ok(())
        })
        .await?;
    let message = match (max_age, max_messages) {
        (Some(max_age), Some(max_messages)) => {
            format!("max age: {}, max messages: {}", max_age, max_messages)
        }
        (None, Some(max_messages)) => format!("max messages: {}", max_messages),
        (Some(max_age), None) => format!("max age: {}", max_age),
        (None, None) => unreachable!(),
    };
    info!("Settings updated for {}: {}", channel_id, message);
    let message = format!("Autodelete settings updated: {}", message);
    let reply = say_reply(ctx, message).await?;
    let reply_id = reply.message().await?.id;
    channel_guard.bot_start_message = Some(reply_id);
    // Drop the lock as soon as we have the reply id
    drop(channel_guard);
    // save the bot_start_message id to the database
    // Use UPDATE because we've made sure the row exists above
    ctx.data()
        .db_connection
        .lock()
        .await
        .call(move |conn| {
            conn.execute(
                "UPDATE channel_settings SET bot_start_message = ? WHERE channel_id = ?",
                params![reply_id.0 as i64, channel_id.0 as i64],
            )?;
            debug!(
                "Updated bot_start_message for {} to {} in database",
                channel_id, reply_id
            );
            Ok(())
        })
        .await?;

    if was_inactive {
        info!(
            "Channel {} wasn't autodeleting, fetching message history",
            channel_id
        );
        fetch_message_history(
            ctx.data().db_connection.clone(),
            ctx.http(),
            channel,
            Direction::Before,
            Some(MessageId::from(ctx.id())),
        )
        .await?;
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
    let channel = ctx.data().channels.remove(ctx.channel_id()).await;
    // Remove from database
    let channel_id = ctx.channel_id();
    // Shouldn't need to do these, but just in case
    ctx.data()
        .db_connection
        .lock()
        .await
        .call(move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "DELETE FROM channel_settings where channel_id = ?",
                params![channel_id.0 as i64],
            )?;
            tx.execute(
                "DELETE FROM message_ids where channel_id = ?",
                params![channel_id.0 as i64],
            )?;
            tx.commit()?;
            debug!("Deleted channel {} from database", channel_id);
            Ok(())
        })
        .await?;
    let message = match channel {
        Some(_) => "Autodelete stopped",
        None => "Autodelete was not active",
    };
    info!("{} for {}", message, channel_id);
    say_reply(ctx, message).await?;
    Ok(())
}

#[poise::command(slash_command, required_permissions = "MANAGE_MESSAGES")]
/// Show autodelete settings for the current channel
async fn status(ctx: Context<'_>) -> Result<(), Error> {
    let channel = ctx.data().channels.get(ctx.channel_id()).await;
    let message = match channel {
        Some(channel) => {
            let channel_guard = channel.0.inner.lock().await;
            match (channel_guard.max_age, channel_guard.max_messages) {
                (Some(max_age), Some(max_messages)) => {
                    format!("max age: {}, max messages: {}", max_age, max_messages)
                }
                (None, Some(max_messages)) => format!("max messages: {}", max_messages),
                (Some(max_age), None) => format!("max age: {}", max_age),
                (None, None) => "Autodelete is not active".to_string(),
            }
        }
        None => "Autodelete is not active".to_string(),
    };
    debug!("Status for {}: {}", ctx.channel_id(), message);
    say_reply(ctx, message).await?;
    Ok(())
}

async fn delete_message(
    http: &Http,
    channel_id: ChannelId,
    message: Message,
    channel: &Channel,
    progress: &usize,
    total: &usize,
) {
    debug!("Deleting message {} [{}/{}]", message, progress, total);
    let delete = channel_id.delete_message(http, message.id).await;
    match delete {
        Err(serenity::Error::Http(e)) if e.status_code() == Some(StatusCode::NOT_FOUND) => {
            // Message was already deleted
            warn!("404: Message {} was already deleted", message);
        }
        Err(e) => {
            error!("Error deleting message: {}", e);
            debug!("Returning message to queue");
            channel.0.inner.lock().await.delete_queue.push(message);
            channel.0.delete_notify.notify_one();
        }
        Ok(_) => {}
    }
}

// Task to save the message IDs periodically
// This works on all channels in one sweep, to minimize the number of database
// calls. And also to have the disk writes be grouped together.
fn save_task(channels: Channels) {
    tokio::spawn(async move {
        // Use a separate connection for the save task
        debug!("Starting save task");
        let db_connection = Connection::open("autodelete.db").await.unwrap();
        // Get sleep time from environment, defaults to 5 minutes
        let save_interval = std::env::var("SAVE_INTERVAL")
            .map(|x| x.parse::<u64>().expect("SAVE_INTERVAL must be a number"))
            .unwrap_or(300);
        loop {
            debug!("Saving message IDs");
            let channels_local = channels.to_cloned_vec().await;
            save_all_message_ids(&db_connection, channels_local).await;
            info!("Saved message IDs");

            tokio::time::sleep(std::time::Duration::from_secs(save_interval)).await;
        }
    });
}

async fn save_message_ids(db_connection: Arc<Mutex<Connection>>, channel: Channel) {
    let channel_id = channel.0.channel_id;
    debug!("Saving message IDs for {}", channel_id);
    db_connection
        .lock()
        .await
        .call(move |conn| {
            let channel_guard = channel.0.inner.blocking_lock();
            let tx = conn.transaction()?;
            // Delete instead of truncate, so the transaction can be
            // rolled back.
            tx.execute(
                "DELETE FROM message_ids WHERE channel_id = ?",
                [channel_id.0 as i64],
            )?;
            let mut insert_id =
                tx.prepare("INSERT INTO message_ids (channel_id, message_id, attachments, embeds) VALUES (?, ?, ?, ?)")?;
            for message in channel_guard.messages.iter() {
                insert_id.execute(params![channel_id.0 as i64, message.id.0 as i64, message.attachments, message.embeds])?;
            }
            drop(insert_id);
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    debug!("Channel {} done saving", channel_id);
}

async fn save_all_message_ids(
    db_connection: &Connection,
    channels_local: Vec<(ChannelId, Channel)>,
) {
    let mut all_ids = Vec::new();
    let mut all_last_seens = Vec::new();
    // Using for loop because async closures aren't stable yet
    for (channel_id, channel) in &channels_local {
        let channel_guard = channel.0.inner.lock().await;
        for message_id in &channel_guard.messages {
            all_ids.push((*channel_id, message_id.clone()));
        }
        all_last_seens.push((*channel_id, channel_guard.last_seen_message));
    }

    db_connection
        .call(move |conn| {
            let tx = conn.transaction()?;
            // Delete instead of truncate, so the transaction can be
            // rolled back.
            tx.execute("DELETE FROM message_ids", [])?;
            let mut insert_id =
                tx.prepare("INSERT INTO message_ids (channel_id, message_id, attachments, embeds) VALUES (?, ?, ?, ?)")?;
            let mut update_last_seen = tx.prepare(
                "UPDATE channel_settings SET last_seen_message = ? WHERE channel_id = ?",
            )?;
            for (channel_id, message) in &all_ids {
                insert_id.execute(params![channel_id.0 as i64, message.id.0 as i64, message.attachments, message.embeds])?;
            }
            for (channel_id, last_seen) in &all_last_seens {
                update_last_seen.execute(params![
                    last_seen.map(|id| id.0 as i64),
                    channel_id.0 as i64,
                ])?;
            }
            drop(insert_id);
            drop(update_last_seen);
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn event_event_handler(
    ctx: &serenity::prelude::Context,
    event: &poise::Event<'_>,
    _framework: poise::FrameworkContext<'_, Data, Error>,
    user_data: &Data,
) -> Result<(), Error> {
    match event {
        poise::Event::Ready { data_about_bot } => {
            info!("{} is connected!", data_about_bot.user.name);
            let http = ctx.http.clone();

            // Catch up on messages that have happened since the bot was last online
            let channels = user_data.channels.to_cloned_vec().await;
            for (_, channel) in channels {
                let channel_guard = channel.0.inner.lock().await;
                let last_seen = channel_guard.last_seen_message;
                drop(channel_guard);
                let http = http.clone();
                let db_connection = user_data.db_connection.clone();
                tokio::spawn(async move {
                    fetch_message_history(
                        db_connection,
                        http,
                        channel,
                        Direction::After,
                        last_seen,
                    )
                    .await
                    .unwrap();
                });
            }
        }
        poise::Event::Message { new_message } => {
            let embeds_len = new_message.embeds.len();
            let attachments_len = new_message.attachments.len();
            debug!(
                "Received message {} on channel {}: {} ({} embeds, {} attachments)",
                new_message.id,
                new_message.channel_id,
                new_message.content,
                embeds_len,
                attachments_len
            );
            for (index, embed) in new_message.embeds.iter().enumerate() {
                debug!("  Embed[{}/{}]: {:?}", index, embeds_len, embed);
            }
            for (index, attachment) in new_message.attachments.iter().enumerate() {
                debug!(
                    "  Attachment[{}/{}]: {:?}",
                    index, attachments_len, attachment
                );
            }
            let channel = user_data.channels.get(new_message.channel_id).await;
            let channel = match channel {
                Some(channel) => channel,
                None => {
                    debug!("Ignoring message in channel without settings");
                    return Ok(());
                }
            };
            let mut channel_guard = channel.0.inner.lock().await;
            if channel_guard.max_age.is_none() && channel_guard.max_messages.is_none() {
                error!("Ignoring message in channel without both maxes (shouldn't happen?)");
                return Ok(());
            }
            if let Some(bot_start_message) = channel_guard.bot_start_message {
                if bot_start_message == new_message.id {
                    debug!("Ignoring bot start message");
                    return Ok(());
                }
            }
            channel_guard.last_seen_message = Some(new_message.id);
            let was_empty = channel_guard.messages.is_empty();
            channel_guard.messages.push_back(Message {
                id: new_message.id,
                attachments: !new_message.attachments.is_empty(),
                embeds: !new_message.embeds.is_empty(),
            });
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

            log_message_id(user_data, new_message.channel_id, new_message.id).await?;
        }
        _ => (),
    }

    Ok(())
}

async fn log_message_id(
    data: &Data,
    channel_id: ChannelId,
    message_id: MessageId,
) -> Result<(), Error> {
    data.db_connection
        .lock()
        .await
        .call(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO message_log (channel_id, message_id) VALUES (?, ?)",
                params![channel_id.0 as i64, message_id.0 as i64],
            )?;
            debug!("Logged message {} to database", message_id);
            Ok(())
        })
        .await?;
    Ok(())
}

async fn log_message_ids(
    db_connection: Arc<Mutex<Connection>>,
    channel_id: ChannelId,
    messages: &[Message],
) -> Result<(), Error> {
    let messages = messages.to_vec();
    db_connection
        .lock()
        .await
        .call(move |conn| {
            let tx = conn.transaction()?;
            let mut statement = tx.prepare(
                "INSERT OR IGNORE INTO message_log (channel_id, message_id) VALUES (?, ?)",
            )?;
            for message in messages {
                statement.execute(params![channel_id.0 as i64, message.id.0 as i64])?;
            }
            drop(statement);
            tx.commit()?;
            debug!("Saved messages to log");
            Ok(())
        })
        .await?;
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

async fn exit_handler(channels: Channels, db_connection: Connection) {
    tokio::spawn(async move {
        wait_for_termination().await;
        info!("Signal received, shutting down.");
        // We hang on to the write lock until the end of the function, so
        // that nothing can be added, nor that the periodic save task can
        // run before we exit.
        let channels_guard = channels.0.write().await;
        let channels_local = channels_guard
            .iter()
            .map(|(id, channel)| (*id, channel.clone()))
            .collect::<Vec<_>>();
        // Stop the tasks on the channels
        debug!("Stopping tasks");
        for (_, channel) in channels_local.iter() {
            let mut channel_guard = channel.0.inner.lock().await;
            channel_guard.stop_tasks = true;
            channel.0.delete_notify.notify_one();
            channel.0.expire_notify.notify_one();
        }
        // Wait for all the channels' tasks to finish
        debug!("Waiting for tasks to finish...");
        for (_, channel) in channels_local.iter() {
            loop {
                let channel_guard = channel.0.inner.lock().await;
                if channel_guard.delete_task_stopped && channel_guard.expire_task_stopped {
                    break;
                }
                drop(channel_guard);
                debug!("Waiting for tasks to finish (100ms sleep)");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        debug!("Tasks finished");
        // Return messages from delete queues to message IDs
        debug!("Returning messages from delete queues");
        for (channel_id, channel) in channels_local.iter() {
            let mut channel_guard = channel.0.inner.lock().await;
            debug!(
                "Channel {} has {} messages in delete queue",
                channel_id,
                channel_guard.delete_queue.len()
            );
            while let Some(message_id) = channel_guard.delete_queue.pop() {
                channel_guard.messages.push_back(message_id);
            }
        }
        save_all_message_ids(&db_connection, channels_local).await;
        warn!("Saved message IDs, exiting due to signal.");
        std::process::exit(0);
    });
}

fn try_sql(conn: &rusqlite::Connection, sql: &str) -> Result<(), rusqlite::Error> {
    let result = conn.execute(sql, []);
    if let Err(SqliteFailure(_, ref reason)) = result {
        if reason
            .as_ref()
            .map(|s| s.contains("duplicate column"))
            .unwrap_or(false)
        {
            debug!("Column attachments_max_age already exists, skipping");
        } else {
            return result.map(|_| ());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::init();
    info!("Starting up");

    let db_connection = Connection::open("autodelete.db").await.unwrap();

    let channels = db_connection
        .call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS channel_settings (
                    channel_id INTEGER PRIMARY KEY,
                    max_age INTEGER,
                    max_messages INTEGER,
                    last_seen_message INTEGER,
                    bot_start_message INTEGER
                )",
                [],
            )?;
            try_sql(
                conn,
                "ALTER TABLE channel_settings
                ADD attachments_max_age INTEGER",
            )?;
            try_sql(
                conn,
                "ALTER TABLE channel_settings
                ADD attachments_max_messages INTEGER",
            )?;
            try_sql(
                conn,
                "ALTER TABLE channel_settings
                ADD embeds_max_age INTEGER",
            )?;
            try_sql(
                conn,
                "ALTER TABLE channel_settings
                ADD embeds_max_messages INTEGER",
            )?;
            let mut channel_settings = conn.prepare(
                "SELECT channel_id,
                    max_age,
                    max_messages,
                    attachments_max_age,
                    attachments_max_messages,					
                    embeds_max_age,
                    embeds_max_messages,
                    last_seen_message,
                    bot_start_message
                    FROM channel_settings",
            )?;
            let channel_settings: HashMap<ChannelId, ChannelInner> = channel_settings
                .query_map([], |row| {
                    let channel_id: i64 = row.get(0)?;
                    let max_age: Option<i64> = row.get(1)?;
                    let max_messages: Option<i64> = row.get(2)?;
                    let attachments_max_age: Option<i64> = row.get(3)?;
                    let attachments_max_messages: Option<i64> = row.get(4)?;
                    let embeds_max_age: Option<i64> = row.get(5)?;
                    let embeds_max_messages: Option<i64> = row.get(6)?;
                    let last_seen_message: Option<i64> = row.get(7)?;
                    let bot_start_message: Option<i64> = row.get(8)?;
                    Ok((
                        ChannelId(channel_id as u64),
                        ChannelInner {
                            max_age: max_age.map(Duration::seconds),
                            max_messages: max_messages.map(|x| x as u64),
                            attachments_max_age: attachments_max_age.map(Duration::seconds),
                            attachments_max_messages: attachments_max_messages.map(|x| x as u64),
                            embeds_max_age: embeds_max_age.map(Duration::seconds),
                            embeds_max_messages: embeds_max_messages.map(|x| x as u64),
                            last_seen_message: last_seen_message
                                .map(|id| MessageId::from(id as u64)),
                            bot_start_message: bot_start_message
                                .map(|id| MessageId::from(id as u64)),
                            ..Default::default()
                        },
                    ))
                })?
                .collect::<Result<HashMap<ChannelId, ChannelInner>, rusqlite::Error>>()?;
            Ok(channel_settings)
        })
        .await
        .unwrap();
    info!("Loaded channel settings from database");
    debug!("Channel settings:");
    channels.iter().for_each(|(k, v)| {
        debug!("{}: {:?}", k, v);
    });

    let mut message_ids = db_connection
        .call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS message_ids (
                    channel_id INTEGER,
                    message_id INTEGER,
                    PRIMARY KEY (channel_id, message_id)
                )",
                [],
            )?;
            try_sql(
                conn,
                "ALTER TABLE message_ids
                ADD COLUMN attachments BOOLEAN",
            )?;
            try_sql(
                conn,
                "ALTER TABLE message_ids
                ADD COLUMN embeds BOOLEAN",
            )?;
            let mut message_ids_query = conn.prepare(
                "SELECT channel_id, message_id, attachments, embeds
                        FROM message_ids
                        ORDER BY channel_id, message_id",
            )?;
            let mut message_ids: HashMap<ChannelId, VecDeque<MessageId>> = HashMap::new();
            for row in message_ids_query.query_map([], |row| {
                let channel_id: i64 = row.get(0)?;
                let message_id: i64 = row.get(1)?;
                let attachments: Option<bool> = row.get(2)?;
                let embeds: Option<bool> = row.get(3)?;
                Ok((
                    ChannelId(channel_id as u64),
                    MessageId(message_id as u64),
                    attachments.unwrap_or(false),
                    embeds.unwrap_or(false),
                ))
            })? {
                let (channel_id, message_id, attachments, embeds) = row?;
                let message_ids = message_ids.entry(channel_id).or_default();
                message_ids.push_back(message_id);
            }
            info!("Loaded message IDs from database");
            debug!("Loaded message IDs: {:?}", message_ids);
            Ok(message_ids)
        })
        .await
        .unwrap();

    db_connection
        .call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS message_log (
                channel_id INTEGER,
                message_id INTEGER,
                PRIMARY KEY (channel_id, message_id)
            )",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![autodelete()],
            event_handler: |ctx, event, framework, user_data| {
                Box::pin(event_event_handler(ctx, event, framework, user_data))
            },
            ..Default::default()
        })
        .token(std::env::var("DISCORD_TOKEN").expect("missing DISCORD_TOKEN"))
        .intents(
            GatewayIntents::GUILD_MESSAGES
                | GatewayIntents::GUILDS
                | GatewayIntents::MESSAGE_CONTENT,
        )
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;

                // Converting from ChannelInner to Channel requires the Http client, so we do it here
                let channels = channels
                    .into_iter()
                    .map(|(channel_id, mut inner)| {
                        inner.messages = message_ids.remove(&channel_id).unwrap_or_default();
                        (
                            channel_id,
                            Channel::new(ctx.http.clone(), channel_id, inner),
                        )
                    })
                    .collect::<HashMap<ChannelId, Channel>>();
                let channels = Channels(Arc::new(RwLock::new(channels)));
                // background task for periodic saving
                save_task(channels.clone());
                exit_handler(channels.clone(), db_connection.clone()).await;
                let data = Data {
                    db_connection: Arc::new(Mutex::new(db_connection)),
                    channels,
                };
                Ok(data)
            })
        });
    info!("Initialization complete. Starting framework!");
    framework.run().await.unwrap();
}
