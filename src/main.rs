use poise::serenity_prelude::{MessageId, Timestamp};
use rusqlite::params;
use serenity::model::prelude::{ChannelId, GatewayIntents};
use serenity::prelude::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio_rusqlite::Connection;

#[derive(Clone, Debug)]
struct ChannelSettings {
    max_messages: Option<u64>,
    max_age: Option<Duration>,
}

struct Data {
    // User data, which is stored and accessible in all command invocations
    channel_settings: Arc<RwLock<HashMap<ChannelId, ChannelSettings>>>,
    message_ids_map: Arc<RwLock<HashMap<ChannelId, Arc<Mutex<VecDeque<MessageId>>>>>>,
    delete_queues: Arc<RwLock<HashMap<ChannelId, Arc<Mutex<Vec<MessageId>>>>>>,
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
        .map(|max_age| duration_str::parse_time(max_age))
        .transpose();
    if max_age.is_err() {
        return Err("Invalid max age, use a number followed by a unit (s, m, h, d)".into());
    }
    let max_age = max_age.unwrap();
    if let Some(max_age) = max_age {
        if max_age > Duration::days(13) {
            return Err("Max age must be 13 days or less".into());
        }
    }
    if max_age.is_none() && max_messages.is_none() {
        return Err("Must specify at least one of max messages or max age".into());
    }
    ctx.data().channel_settings.write().await.insert(
        ctx.channel_id(),
        ChannelSettings {
            max_messages,
            max_age,
        },
    );
    ctx.say("Autodelete settings updated").await?;
    println!("Channel settings:");
    ctx.data()
        .channel_settings
        .read()
        .await
        .iter()
        .for_each(|(k, v)| {
            println!("{}: {:?}", k, v);
        });
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
                    max_age.map(|x| x.whole_seconds() as i64),
                ],
            )?;
            Ok(())
        })
        .await?;
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
            let delete_queues = user_data.delete_queues.clone();

            // Start a background task to delete messages
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let delete_queues_guard = delete_queues.read().await;
                    let delete_queues_local = delete_queues_guard
                        .iter()
                        .map(|(channel_id, delete_queue)| (*channel_id, delete_queue.clone()))
                        .collect::<Vec<_>>();
                    drop(delete_queues_guard);
                    for (channel_id, delete_queue) in delete_queues_local.into_iter() {
                        let mut delete_queue_guard = delete_queue.lock().await;
                        let mut delete_queue_local = Vec::new();
                        delete_queue_local.append(&mut delete_queue_guard);
                        drop(delete_queue_guard);
                        match delete_queue_local.len() {
                            0 => continue,
                            1 => {
                                let result = channel_id
                                    .delete_message(&http, delete_queue_local[0])
                                    .await;
                                if result.is_err() {
                                    println!("Error deleting message: {:?}", result);
                                    delete_queue.lock().await.push(delete_queue_local[0]);
                                }
                            }
                            _ => {
                                for chunk in delete_queue_local.chunks(100) {
                                    let result =
                                        channel_id.delete_messages(&http, chunk.iter()).await;
                                    if result.is_err() {
                                        println!("Error deleting messages: {:?}", result);
                                        delete_queue.lock().await.extend(chunk);
                                    }
                                }
                            }
                        }
                    }
                }
            });

            // Start a background task to put expired messages in the delete queue
            let message_ids_map = user_data.message_ids_map.clone();
            let channel_settings = user_data.channel_settings.clone();
            let delete_queues = user_data.delete_queues.clone();
            tokio::spawn(async move {
                loop {
                    // snag a copy of the message_ids_map
                    let message_ids_map_guard = message_ids_map.read().await;
                    let message_ids_map_local = message_ids_map_guard
                        .iter()
                        .map(|(k, v)| (*k, v.clone()))
                        .collect::<Vec<_>>();
                    drop(message_ids_map_guard);
                    // snag a copy of the channel_settings
                    let channel_settings_guard = channel_settings.read().await;
                    let channel_settings_local = channel_settings_guard.clone();
                    drop(channel_settings_guard);
                    for (channel_id, message_ids) in message_ids_map_local.into_iter() {
                        let settings = channel_settings_local.get(&channel_id);
                        // If there are no settings or no max-age for this channel, toss the message ids
                        if settings.is_none() || settings.unwrap().max_age.is_none() {
                            message_ids_map.write().await.remove(&channel_id);
                            continue;
                        }
                        // should work because of the continue above
                        let max_age = settings.unwrap().max_age.unwrap();
                        let message_ids_guard = message_ids.lock().await;
                        let now = OffsetDateTime::now_utc();
                        let to_delete = message_ids_guard
                            .iter()
                            .filter(|message_id| message_id.created_at() < Timestamp::from(now - max_age))
                            .map(|message_id| *message_id)
                            .collect::<Vec<_>>();
                        drop(message_ids_guard);
                        let delete_queue = delete_queues
                            .write()
                            .await
                            .entry(channel_id)
                            .or_default()
                            .clone();
                        delete_queue.lock().await.extend(to_delete);
                    }
                }
                // TODO: sleep
                
            });
        }
        poise::Event::Message { new_message } => {
            println!("{}: {}", new_message.author.name, new_message.content);
            // Check if there are settings for the channel
            let channel_settings = user_data.channel_settings.read().await;
            let channel_settings = channel_settings.get(&new_message.channel_id);
            if !user_data
                .channel_settings
                .read()
                .await
                .contains_key(&new_message.channel_id)
            {
                return Ok(());
            }
            // If it does, copy out the max_messages setting
            let max_messages = channel_settings.unwrap().max_messages;
            drop(channel_settings);
            // Need write lock to add a VecDeque to the map if it doesn't exist yet
            let mut message_ids_map = user_data.message_ids_map.write().await;
            let message_ids = message_ids_map
                .entry(new_message.channel_id)
                .or_default()
                .clone();
            // Since we cloned the Arc around the Mutex & VecDeque, we can drop the write lock
            drop(message_ids_map);
            println!(
                "adding message id {:?} to channel {:?}",
                new_message.id, new_message.channel_id
            );
            let mut message_ids = message_ids.lock().await;
            let mut to_delete = Vec::new();
            message_ids.push_back(new_message.id);
            println!(
                "Max messages: {:?}, num msgs: {}",
                max_messages,
                message_ids.len()
            );
            if let Some(max_messages) = max_messages {
                while message_ids.len() > max_messages as usize {
                    let message_id = message_ids.pop_front().unwrap();
                    println!("Deleting message {}", message_id);
                    to_delete.push(message_id);
                }
            }
            drop(message_ids);
            // Add to delete queues
            let mut delete_queues_guard = user_data.delete_queues.write().await;
            let delete_queue = delete_queues_guard
                .entry(new_message.channel_id)
                .or_default()
                .clone();
            // Again, we can drop the write lock to the whole map, since we have a clone of the Arc
            println!("Adding to delete queue: {:?}", to_delete);
            println!("Delete queues: {:?}", delete_queues_guard);
            drop(delete_queues_guard);
            let mut delete_queue = delete_queue.lock().await;
            delete_queue.extend(to_delete);
        }
        _ => {}
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let db_connection = Connection::open("autodelete.db").await.unwrap();

    let channel_settings = db_connection
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
            let channel_settings: HashMap<ChannelId, ChannelSettings> = channel_settings
                .query_map([], |row| {
                    let channel_id: i64 = row.get(0)?;
                    let max_messages: Option<i64> = row.get(1)?;
                    let max_age: Option<i64> = row.get(2)?;
                    Ok((
                        ChannelId(channel_id as u64),
                        ChannelSettings {
                            max_messages: max_messages.map(|x| x as u64),
                            max_age: max_age.map(|x| Duration::seconds(x)),
                        },
                    ))
                })?
                .collect::<Result<HashMap<ChannelId, ChannelSettings>, rusqlite::Error>>()?;
            Ok(channel_settings)
        })
        .await
        .unwrap();
    println!("Channel settings:");
    channel_settings.iter().for_each(|(k, v)| {
        println!("{}: {:?}", k, v);
    });

    let message_ids = db_connection
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
            let mut message_ids: HashMap<ChannelId, Arc<Mutex<VecDeque<MessageId>>>> =
                HashMap::new();
            for row in message_ids_query.query_map([], |row| {
                let channel_id: i64 = row.get(0)?;
                let message_id: i64 = row.get(1)?;
                Ok((ChannelId(channel_id as u64), MessageId(message_id as u64)))
            })? {
                let (channel_id, message_id) = row?;
                let message_ids = message_ids.entry(channel_id).or_default();
                message_ids.blocking_lock().push_back(message_id);
            }
            println!("Message IDs: {:?}", message_ids);
            Ok(message_ids)
        })
        .await
        .unwrap();

    let data = Data {
        db_connection: Arc::new(Mutex::new(db_connection)),
        channel_settings: Arc::new(RwLock::new(channel_settings)),
        delete_queues: Arc::new(RwLock::new(HashMap::new())),
        message_ids_map: Arc::new(RwLock::new(message_ids)),
    };
    let message_ids_map = data.message_ids_map.clone();
    tokio::spawn(async move {
        let db_connection = Connection::open("autodelete.db").await.unwrap();
        loop {
            let message_ids_map = message_ids_map.read().await;
            let message_ids_map_copy = message_ids_map
                .iter()
                .map(|(channel_id, message_ids)| (*channel_id, message_ids.clone()))
                .collect::<Vec<_>>();
            drop(message_ids_map);
            db_connection
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    tx.execute("DELETE FROM message_ids", [])?;
                    let mut insert = tx.prepare(
                        "INSERT INTO message_ids (channel_id, message_id) VALUES (?, ?)",
                    )?;
                    for (channel_id, message_ids) in message_ids_map_copy {
                        let message_ids = message_ids.blocking_lock();
                        for message_id in message_ids.iter() {
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

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
