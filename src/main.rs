/// A Discord bot that deletes messages after a certain amount of time or when a maximum number of messages is reached.
///
/// This is inspired by https://github.com/riking/AutoDelete
/// Its maintainer is on extended hiatus, so here's a rewrite in Rust.
///
/// Some improvements over the original:
/// * Uses slash commands in addition to regular commands
///
/// TODO:
/// * Support for rate limiting
/// * Spruce up the help text
/// * Maybe an admin HTTP server with some status info
/// * Catch signals and do a graceful shutdown
/// * Put delete queues back into message id lists when shutting down
use itertools::Itertools;
use poise::say_reply;
use poise::serenity_prelude::routing::Route;
use poise::serenity_prelude::{CacheHttp, Http, MessageId, StatusCode, Timestamp};
use rusqlite::params;
use serenity::model::prelude::{ChannelId, GatewayIntents};
use serenity::prelude::{Mutex, RwLock};
use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::time::sleep;
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

#[derive(Debug, Default)]
struct Channel {
    max_age: Option<Duration>,
    max_messages: Option<u64>,
    message_ids: VecDeque<MessageId>,
    delete_queue: Vec<MessageId>,
    last_seen_message: Option<MessageId>,
    bot_start_message: Option<MessageId>,
}

impl Channel {
    fn check_expiry(&mut self) {
        if let Some(max_age) = self.max_age {
            let now = OffsetDateTime::now_utc();
            let oldest_allowed = Timestamp::from(now - max_age);
            let mut keep = VecDeque::new();
            let mut ids = self.message_ids.iter();
            loop {
                let id = ids.next();
                if id.is_none() {
                    break;
                }
                let id = id.unwrap();
                if let Some(bot_start_message) = self.bot_start_message {
                    if *id == bot_start_message {
                        // We want to keep the bot's start message, so just ignore its
                        // existence and continue to the next message.
                        // It could have been added by a fetch of history.
                        continue;
                    }
                }
                if id.created_at() < oldest_allowed {
                    self.delete_queue.push(*id);
                } else {
                    keep.push_back(*id);
                    // messages should be sorted by timestamp, so we can stop here
                    // and extend() with remainder
                    break;
                }
            }
            keep.extend(ids);
            self.message_ids.clear();
            self.message_ids.append(&mut keep);
        }
    }

    fn check_max_messages(&mut self) {
        if let Some(max_messages) = self.max_messages {
            for _ in 0..self.message_ids.len() as isize - max_messages as isize {
                self.delete_queue
                    .push(self.message_ids.pop_front().unwrap());
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Channels(Arc<RwLock<HashMap<ChannelId, Arc<Mutex<Channel>>>>>);

impl Channels {
    async fn get_or_default(&self, channel_id: ChannelId) -> Arc<Mutex<Channel>> {
        // Most get() calls will be for channels that already exist, so we
        // use read() first to avoid locking write(). Bind the guard so we
        // can explicitly drop it before locking write() if needed. Without
        // the explicit drop, we would get a deadlock.
        let channels_guard = self.0.read().await;
        match channels_guard.get(&channel_id).cloned() {
            Some(channel) => channel,
            None => {
                let channel = Arc::new(Mutex::new(Channel::default()));
                drop(channels_guard);
                self.0.write().await.insert(channel_id, channel.clone());
                channel
            }
        }
    }

    async fn get(&self, channel_id: ChannelId) -> Option<Arc<Mutex<Channel>>> {
        self.0.read().await.get(&channel_id).cloned()
    }

    async fn remove(&self, channel_id: ChannelId) -> Option<Arc<Mutex<Channel>>> {
        self.0.write().await.remove(&channel_id)
    }

    async fn to_cloned_vec(&self) -> Vec<(ChannelId, Arc<Mutex<Channel>>)> {
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
    http: impl AsRef<Http>,
    channel_id: ChannelId,
    channel: Arc<Mutex<Channel>>,
    direction: Direction,
    message_id: Option<MessageId>,
) -> Result<(), Error> {
    println!(
        "Fetching message history for channel {}. Requesting messages {} {}",
        channel_id,
        direction,
        message_id.unwrap_or_default()
    );
    let mut origin = message_id;
    let mut message_ids: Vec<MessageId> = Vec::new();
    // If there's no message_id, we're fetching the most recent messages
    // so always go backwards in that case.
    let direction = if message_id.is_none() {
        Direction::Before
    } else {
        direction
    };
    loop {
        println!(
            "Messages ratelimit: {:?}",
            http.as_ref()
                .ratelimiter
                .routes()
                .read()
                .await
                .get(&Route::ChannelsIdMessages(channel_id.into()))
                .cloned()
        );
        let messages = channel_id
            .messages(&http, |b| match (origin, direction) {
                (Some(origin), Direction::After) => b.after(origin).limit(100),
                (Some(origin), Direction::Before) => b.before(origin).limit(100),
                (None, _) => b.limit(100),
            })
            .await?;
        println!(
            "Messages ratelimit: {:?}",
            http.as_ref()
                .ratelimiter
                .routes()
                .read()
                .await
                .get(&Route::ChannelsIdMessages(channel_id.into()))
                .cloned()
        );
        if messages.is_empty() {
            break;
        }
        if direction == Direction::After {
            origin = Some(messages.first().unwrap().id);
        } else {
            origin = Some(messages.last().unwrap().id);
        }
        message_ids.extend(messages.into_iter().map(|m| m.id));
    }
    println!(
        "Fetched {} messages for channel {}",
        message_ids.len(),
        channel_id
    );
    // Let's check some assumptions
    let mut sorted_ids = message_ids.clone();
    sorted_ids.sort_unstable_by_key(|w| Reverse(w.0));
    if sorted_ids != message_ids {
        println!("Messages (history) were not returned in order");
    }
    sorted_ids.dedup();
    if sorted_ids != message_ids {
        println!("Messages (history) were not unique");
    }
    // Add to the channel's data
    // The messages are currently going from newest to oldest, but we want
    // them in the opposite order, because that's how the channel data has
    // them. Since messages might have arrived while we retrieved history,
    // those will already be in the channel data (BEFORE). Or we're just
    // starting up and getting stuff we missed (AFTER).
    sorted_ids.reverse();
    let mut ids = VecDeque::from(sorted_ids);
    let mut channel_guard = channel.lock().await;
    if direction == Direction::Before {
        // We're getting history from before what we know, so put
        // the new messages at the front. Do that by appending what
        // we might have to the just retrieved messages, and then
        // putting the whole result back.
        ids.append(&mut channel_guard.message_ids);
    }
    // Remove duplicates that might have been added by the message
    // event handler while we were retrieving history
    channel_guard.message_ids.extend(ids.into_iter().dedup());
    channel_guard.message_ids.make_contiguous().sort_unstable();
    channel_guard.check_max_messages();
    Ok(())
}

/// The bot's main entry point
#[poise::command(
    prefix_command,
    slash_command,
    subcommands("start", "stop", "status", "help")
)]
async fn autodelete(ctx: Context<'_>) -> Result<(), Error> {
    poise::builtins::help(ctx, None, Default::default()).await?;
    Ok(())
}

/// Show help for the autodelete command
#[poise::command(prefix_command, slash_command)]
async fn help(ctx: Context<'_>) -> Result<(), Error> {
    poise::builtins::help(ctx, None, Default::default()).await?;
    Ok(())
}

/// Update autodelete settings for the current channel
#[poise::command(
    slash_command,
    prefix_command,
    required_permissions = "MANAGE_MESSAGES"
)]
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
    // Discord allows deleting of messages through the API up to 14 days old, so
    // we take an extra day as buffer.
    if let Some(max_age) = max_age {
        if max_age > Duration::days(13) {
            return Err("Max age must be 13 days or less".into());
        }
    }
    if max_age.is_none() && max_messages.is_none() {
        return Err("Must specify at least one of max messages or max age".into());
    }
    let channel = ctx.data().channels.get_or_default(ctx.channel_id()).await;
    let mut channel_guard = channel.lock().await;
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
            Ok(())
        })
        .await?;

    if was_inactive {
        fetch_message_history(
            &ctx.http(),
            channel_id,
            channel,
            Direction::Before,
            Some(MessageId::from(ctx.id())),
        )
        .await?;
    }
    Ok(())
}

/// Stop autodelete for the current channel
#[poise::command(
    slash_command,
    prefix_command,
    required_permissions = "MANAGE_MESSAGES"
)]
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
            Ok(())
        })
        .await?;
    let message = match channel {
        Some(_) => "Autodelete stopped",
        None => "Autodelete was not active",
    };
    say_reply(ctx, message).await?;
    Ok(())
}

/// Show autodelete settings for the current channel
#[poise::command(
    slash_command,
    prefix_command,
    required_permissions = "MANAGE_MESSAGES"
)]
async fn status(ctx: Context<'_>) -> Result<(), Error> {
    let channel = ctx.data().channels.get(ctx.channel_id()).await;
    let message = match channel {
        Some(channel) => {
            let channel_guard = channel.lock().await;
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
    say_reply(ctx, message).await?;
    Ok(())
}

async fn delete_message(
    http: &Http,
    channel_id: ChannelId,
    message_id: MessageId,
    channel: &Arc<Mutex<Channel>>,
) {
    let delete = channel_id.delete_message(http, message_id).await;
    match delete {
        Err(serenity::Error::Http(e)) if e.status_code() == Some(StatusCode::NOT_FOUND) => {
            // Message was already deleted
            println!("404: Message {} was already deleted", message_id);
        }
        Err(e) => {
            eprintln!("Error deleting message: {}", e);
            channel.lock().await.delete_queue.push(message_id);
        }
        Ok(_) => {}
    }
}

// background task to put expired messages in the delete queue
fn expire_task(channels: Channels) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            // snag a copy of the message_ids_map
            let channels_local = channels.to_cloned_vec().await;
            for (_, channel) in channels_local.into_iter() {
                let mut channel = channel.lock().await;
                channel.check_expiry();
            }
        }
    });
}

// Background task to delete messages
// This is intentionally separate
fn delete_task(http: Arc<Http>, channels: Channels) {
    tokio::spawn(async move {
        loop {
            sleep(std::time::Duration::from_millis(500)).await;
            let channels_local = channels.to_cloned_vec().await;
            for (channel_id, channel) in channels_local.into_iter() {
                let mut delete_queue_local = Vec::new();
                let mut channel_guard = channel.lock().await;
                delete_queue_local.append(&mut channel_guard.delete_queue);
                drop(channel_guard);
                // Messages older than two weeks can't be bulk deleted, so split
                // the queue into two parts
                let two_weeks_ago = Timestamp::from(
                    // Take one hour off to avoid race conditions with clocks that are
                    // a little (up to an hour) out of sync.
                    OffsetDateTime::now_utc() - Duration::weeks(2) + Duration::hours(1),
                );
                let delete_queue_non_bulk = delete_queue_local
                    .iter()
                    .filter(|x| x.created_at() <= two_weeks_ago)
                    .cloned()
                    .collect::<Vec<_>>();
                let delete_queue_bulk = delete_queue_local
                    .into_iter()
                    .filter(|x| x.created_at() > two_weeks_ago)
                    .collect::<Vec<_>>();
                for message_id in delete_queue_non_bulk {
                    delete_message(&http, channel_id, message_id, &channel).await;
                }
                for chunk in delete_queue_bulk.chunks(100) {
                    match chunk.len() {
                        1 => {
                            delete_message(&http, channel_id, chunk[0], &channel).await;
                        }
                        _ => {
                            let result = channel_id.delete_messages(&http, chunk.iter()).await;
                            if result.is_err() {
                                println!("Error deleting messages: {:?}", result);
                                channel.lock().await.delete_queue.extend(chunk);
                            }
                        }
                    }
                }
            }
        }
    });
}

// Task to save the message IDs periodically
// Since this doesn't need a Discord context, it can be started from main()
fn save_task(channels: Channels) {
    tokio::spawn(async move {
        // Use a separate connection for the save task
        let db_connection = Connection::open("autodelete.db").await.unwrap();
        loop {
            let channels_local = channels.to_cloned_vec().await;
            db_connection
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    tx.execute("DELETE FROM message_ids", [])?;
                    let mut insert_id = tx.prepare(
                        "INSERT INTO message_ids (channel_id, message_id) VALUES (?, ?)",
                    )?;
                    let mut update_last_seen = tx.prepare(
                        "UPDATE channel_settings SET last_seen_message = ? WHERE channel_id = ?",
                    )?;
                    for (channel_id, channel) in channels_local {
                        let channel = channel.blocking_lock();
                        for message_id in channel.message_ids.iter() {
                            insert_id.execute([channel_id.0 as i64, message_id.0 as i64])?;
                        }
                        update_last_seen.execute(params![
                            channel.last_seen_message.map(|id| id.0 as i64),
                            channel_id.0 as i64,
                        ])?;
                    }
                    drop(insert_id);
                    drop(update_last_seen);
                    tx.commit()?;
                    println!("Saved message IDs");
                    Ok(())
                })
                .await
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    });
}

async fn event_event_handler(
    ctx: &serenity::prelude::Context,
    event: &poise::Event<'_>,
    _framework: poise::FrameworkContext<'_, Data, Error>,
    user_data: &Data,
) -> Result<(), Error> {
    match event {
        poise::Event::Ready { data_about_bot } => {
            println!("{} is connected!", data_about_bot.user.name);
            let http = ctx.http.clone();
            let channels = user_data.channels.clone();

            delete_task(http.clone(), channels.clone());
            expire_task(channels);

            // Catch up on messages that have happened since the bot was last online
            let channels = user_data.channels.to_cloned_vec().await;
            for (channel_id, channel) in channels {
                let channel_guard = channel.lock().await;
                let last_seen = channel_guard.last_seen_message;
                drop(channel_guard);
                fetch_message_history(ctx.http(), channel_id, channel, Direction::After, last_seen)
                    .await?;
            }
        }
        poise::Event::Message { new_message } => {
            let channel = user_data.channels.get(new_message.channel_id).await;
            let channel = match channel {
                Some(channel) => channel,
                None => return Ok(()),
            };
            let mut channel = channel.lock().await;
            if channel.max_age.is_none() && channel.max_messages.is_none() {
                return Ok(());
            }
            if let Some(bot_start_message) = channel.bot_start_message {
                if bot_start_message == new_message.id {
                    println!("Ignoring bot start message");
                    return Ok(());
                }
            }
            channel.last_seen_message = Some(new_message.id);
            channel.message_ids.push_back(new_message.id);
            channel.check_max_messages();
        }
        _ => (),
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

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
            let mut channel_settings =
                conn.prepare("SELECT channel_id, max_age, max_messages, last_seen_message, bot_start_message FROM channel_settings")?;
            let channel_settings: HashMap<ChannelId, Channel> = channel_settings
                .query_map([], |row| {
                    let channel_id: i64 = row.get(0)?;
                    let max_age: Option<i64> = row.get(1)?;
                    let max_messages: Option<i64> = row.get(2)?;
                    let last_seen_message: Option<i64> = row.get(3)?;
                    let bot_start_message: Option<i64> = row.get(4)?;
                    Ok((
                        ChannelId(channel_id as u64),
                        Channel {
                            max_age: max_age.map(Duration::seconds),
                            max_messages: max_messages.map(|x| x as u64),
                            last_seen_message: last_seen_message.map(|id| MessageId::from(id as u64)),
                            bot_start_message: bot_start_message.map(|id| MessageId::from(id as u64)),
                            ..Default::default()
                        },
                    ))
                })?
                .collect::<Result<HashMap<ChannelId, Channel>, rusqlite::Error>>()?;
            Ok(channel_settings)
        })
        .await
        .unwrap();
    println!("Channel settings:");
    channels.iter().for_each(|(k, v)| {
        println!("{}: {:?}", k, v);
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
            let mut message_ids_query =
                conn.prepare("SELECT channel_id, message_id FROM message_ids")?;
            let mut message_ids: HashMap<ChannelId, VecDeque<MessageId>> = HashMap::new();
            for row in message_ids_query.query_map([], |row| {
                let channel_id: i64 = row.get(0)?;
                let message_id: i64 = row.get(1)?;
                Ok((ChannelId(channel_id as u64), MessageId(message_id as u64)))
            })? {
                let (channel_id, message_id) = row?;
                let message_ids = message_ids.entry(channel_id).or_default();
                message_ids.push_back(message_id);
            }
            println!("Loaded message IDs: {:?}", message_ids);
            Ok(message_ids)
        })
        .await
        .unwrap();

    let channels = channels
        .into_iter()
        .map(|(channel_id, mut channel)| {
            channel.message_ids = message_ids.remove(&channel_id).unwrap_or_default();
            (channel_id, Arc::new(Mutex::new(channel)))
        })
        .collect::<HashMap<ChannelId, Arc<Mutex<Channel>>>>();

    let channels = Channels(Arc::new(RwLock::new(channels)));
    let data = Data {
        db_connection: Arc::new(Mutex::new(db_connection)),
        channels,
    };

    save_task(data.channels.clone());

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
                | GatewayIntents::DIRECT_MESSAGES
                | GatewayIntents::MESSAGE_CONTENT
                | GatewayIntents::GUILDS
                | GatewayIntents::GUILD_MEMBERS
                | GatewayIntents::GUILD_PRESENCES,
        )
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                Ok(data)
            })
        });
    framework.run().await.unwrap();
}
