use poise::serenity_prelude::{MessageId, Timestamp};
use rusqlite::params;
use serenity::model::prelude::{ChannelId, GatewayIntents};
use serenity::prelude::{Mutex, RwLock};
use tokio::time::sleep;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio_rusqlite::Connection;

#[derive(Debug, Default)]
struct Channel {
    max_messages: Option<u64>,
    max_age: Option<Duration>,
    message_ids: VecDeque<MessageId>,
    delete_queue: Vec<MessageId>,
}

impl Channel {
    async fn check_expiry(&mut self) {
        if let Some(max_age) = self.max_age {
            let now = OffsetDateTime::now_utc();
            let oldest_allowed = Timestamp::from(now - max_age);
            let mut keep = VecDeque::new();
            let mut ids = self.message_ids.iter();
            loop {
                let id = ids.next();
                println!("checking message id: {:?}", id);
                if id.is_none() {
                    break;
                }
                let id = id.unwrap();
                if id.created_at() < oldest_allowed {
                    println!("deleting message id: {:?}", id);
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
}

#[derive(Clone, Debug, Default)]
struct Channels(Arc<RwLock<HashMap<ChannelId, Arc<Mutex<Channel>>>>>);

impl Channels {
    async fn get(&self, channel_id: ChannelId) -> Arc<Mutex<Channel>> {
        // Most get() calls will be for channels that already exist, so we
        // use read() first to avoid locking write().
        match self.0.read().await.get(&channel_id).cloned() {
            Some(channel) => channel,
            None => {
                let channel = Arc::new(Mutex::new(Channel::default()));
                self.0.write().await.insert(channel_id, channel.clone());
                channel
            }
        }
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

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

/// Displays your or another user's account creation date
#[poise::command(
    slash_command,
    prefix_command,
    required_permissions = "MANAGE_MESSAGES"
)]
async fn autodelete(
    ctx: Context<'_>,
    #[description = "Max number of messages to keep"] max_messages: Option<u64>,
    #[description = "Max age of messages"] max_age: Option<String>,
) -> Result<(), Error> {
    let max_age = max_age
        .map(duration_str::parse_time)
        .transpose();
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
    let channel = ctx.data().channels.get(ctx.channel_id()).await;
    let mut channel_guard = channel.lock().await;
    channel_guard.max_messages = max_messages;
    channel_guard.max_age = max_age;
    drop(channel_guard);

    // Write to database
    let channel_id = ctx.channel_id();
    ctx.data()
        .db_connection
        .lock()
        .await
        .call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO channel_settings (channel_id, max_messages, max_age) VALUES (?, ?, ?)",
                params![
                    channel_id.0 as i64,
                    max_messages.map(|x| x as i64),
                    max_age.map(|x| x.whole_seconds()),
                ],
            )?;
            Ok(())
        })
        .await?;
    let message = match (max_messages, max_age) {
        (Some(max_messages), Some(max_age)) => {
            format!("max messages: {}, max age: {}", max_messages, max_age)
        }
        (Some(max_messages), None) => format!("max messages: {}", max_messages),
        (None, Some(max_age)) => format!("max age: {}", max_age),
        (None, None) => unreachable!(),
    };
    let message = format!("Autodelete settings updated: {}", message);
    ctx.say(message).await?;

    let channels = ctx.data().channels.to_cloned_vec().await;

    for (channel_id, channel) in channels {
        let channel = channel.lock().await;
        println!(
            "channel<{:?}>: max_age: {:?}, max_messages: {:?}",
            channel_id, channel.max_age, channel.max_messages
        );
    }
    Ok(())
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

            // Start a background task to delete messages
            tokio::spawn(async move {
                loop {
                    sleep(std::time::Duration::from_millis(500)).await;
                    let channels_local = channels.to_cloned_vec().await;
                    for (channel_id, channel) in channels_local.into_iter() {
                        let mut delete_queue_local = Vec::new();
                        let mut channel_guard = channel.lock().await;
                        delete_queue_local.append(&mut channel_guard.delete_queue);
                        drop(channel_guard);
                        match delete_queue_local.len() {
                            0 => continue,
                            1 => {
                                let result = channel_id
                                    .delete_message(&http, delete_queue_local[0])
                                    .await;
                                if result.is_err() {
                                    println!("Error deleting message: {:?}", result);
                                    channel
                                        .lock()
                                        .await
                                        .delete_queue
                                        .push(delete_queue_local[0]);
                                }
                            }
                            _ => {
                                for chunk in delete_queue_local.chunks(100) {
                                    let result =
                                        channel_id.delete_messages(&http, chunk.iter()).await;
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

            // Start a background task to put expired messages in the delete queue
            let channels = user_data.channels.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    // snag a copy of the message_ids_map
                    let channels_local = channels.to_cloned_vec().await;
                    for (_, channel) in channels_local.into_iter() {
                        let mut channel = channel.lock().await;
                        channel.check_expiry().await;
                    }
                }
            });
        }
        poise::Event::Message { new_message } => {
            let channel = user_data.channels.get(new_message.channel_id).await;
            let mut channel = channel.lock().await;
            if channel.max_age.is_none() && channel.max_messages.is_none() {
                return Ok(());
            }
            channel.message_ids.push_back(new_message.id);
            if let Some(max_messages) = channel.max_messages {
                for _ in 0..channel.message_ids.len() as isize - max_messages as isize {
                    // since we pushed above, this will always be Some
                    let id = channel.message_ids.pop_front().unwrap();
                    channel.delete_queue.push(id);
                }
            }

            // Since we can't delete messages older than 14 days, no need to keep them longer than that
            let oldest_allowed = Timestamp::from(OffsetDateTime::now_utc() - Duration::days(14));
            while let Some(id) = channel.message_ids.front() {
                if id.created_at() < oldest_allowed {
                    channel.message_ids.pop_front();
                } else {
                    break;
                }
            }
        }
        _ => {}
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
                    max_messages INTEGER,
                    max_age INTEGER
                )",
                [],
            )?;
            let mut channel_settings =
                conn.prepare("SELECT channel_id, max_messages, max_age FROM channel_settings")?;
            let channel_settings: HashMap<ChannelId, Channel> = channel_settings
                .query_map([], |row| {
                    let channel_id: i64 = row.get(0)?;
                    let max_messages: Option<i64> = row.get(1)?;
                    let max_age: Option<i64> = row.get(2)?;
                    Ok((
                        ChannelId(channel_id as u64),
                        Channel {
                            max_messages: max_messages.map(|x| x as u64),
                            max_age: max_age.map(Duration::seconds),
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
    let channels = data.channels.clone();
    tokio::spawn(async move {
        let db_connection = Connection::open("autodelete.db").await.unwrap();
        loop {
            let channels_local = channels.to_cloned_vec().await;
            db_connection
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    tx.execute("DELETE FROM message_ids", [])?;
                    let mut insert = tx.prepare(
                        "INSERT INTO message_ids (channel_id, message_id) VALUES (?, ?)",
                    )?;
                    for (channel_id, channel) in channels_local {
                        let channel = channel.blocking_lock();
                        for message_id in channel.message_ids.iter() {
                            insert.execute([channel_id.0 as i64, message_id.0 as i64])?;
                        }
                    }
                    drop(insert);
                    tx.commit()?;
                    println!("Saved message ids");
                    Ok(())
                })
                .await
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    });

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
