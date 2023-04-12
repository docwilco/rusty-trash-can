use std::env;

use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::model::prelude::MessageId;
use serenity::prelude::*;
use time::Duration;

struct UserIDString;
impl TypeMapKey for UserIDString {
    type Value = String;
}

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        // If message is prepended with <@USER_ID> then we do stuff
        let user_id_string = ctx.data.read().await.get::<UserIDString>().unwrap().clone();
        if !msg.content.starts_with(&user_id_string) {
            // Otherwise ignore the message
            return;
        }
        // Retrieve the channel from the cache
        let channel = match ctx.cache.guild_channel(msg.channel_id) {
            Some(channel) => channel,
            None => {
                println!("Error getting channel from cache");
                return;
            }
        };
        // Check that the user has MANAGE_MESSAGES permission
        let permissions = channel.permissions_for_user(&ctx, &msg.author);
        let has_manage_messages = match permissions {
            Ok(permissions) => {
                if !permissions.manage_messages() {
                    // Respond to the user
                    let _ = msg
                        .channel_id
                        .say(
                            &ctx.http,
                            "You do not have permission (`MANAGE_MESSAGES`) to use this command",
                        )
                        .await;
                    println!("User does not have MANAGE_MESSAGES permission");
                    false
                } else {
                    true
                }
            }
            Err(_) => {
                println!("Error getting permissions for user");
                false
            }
        };
        // Chop off the user id string
        let content = msg.content.trim_start_matches(&user_id_string).trim_start();
        let mut content = content.split_ascii_whitespace();
        let command = content.next();
        let args = content.collect::<Vec<&str>>();
        // Check the command
        match command {
            Some("ping") => {
                let _ = msg.channel_id.say(&ctx.http, "Pong!").await;
            }
            Some("echo") => {
                let _ = msg.channel_id.say(&ctx.http, args.join(" ")).await;
            }
            Some("start") => {
                if !has_manage_messages {
                    return;
                }
                let _ = msg.channel_id.say(&ctx.http, "Starting...").await;

                let two_weeks_ago = MessageId::from(
                    *msg.id.as_u64() - ((Duration::weeks(2).whole_milliseconds() as u64) << 22));
                let messages = msg
                    .channel_id
                    .messages(&ctx.http, |retriever| {
                        retriever.limit(100).around(two_weeks_ago)
                    })
                    .await.unwrap();
                // Print timestamps for first and last message
                let newest_message = messages.first().unwrap();
                let oldest_message = messages.last().unwrap();
                println!(
                    "Newest message: {}",
                    newest_message.id.created_at()
                );
                println!(
                    "Last message: {}",
                    oldest_message.id.created_at()
                );

            }
            _ => {
                let _ = msg
                    .channel_id
                    .say(&ctx.http, "Unknown command. Try `ping` or `echo`")
                    .await;
            }
        }
    }

    // Set a handler to be called on the `ready` event. This is called when a
    // shard is booted, and a READY payload is sent by Discord. This payload
    // contains data like the current user's guild Ids, current user data,
    // private channels, and more.
    //
    // In this case, just print what the current user's username is.
    async fn ready(&self, ctx: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
        println!("Looking for <@{}>", ready.user.id);
        let user_string = format!("<@{}>", ready.user.id);
        ctx.data.write().await.insert::<UserIDString>(user_string);
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    // Configure the client with your Discord bot token in the environment.
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    // Set gateway intents, which decides what events the bot will be notified about
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT
        | GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MEMBERS
        | GatewayIntents::GUILD_PRESENCES;

    // Create a new instance of the Client, logging in as a bot. This will
    // automatically prepend your bot token with "Bot ", which is a requirement
    // by Discord for bot users.
    let mut client = Client::builder(&token, intents)
        .event_handler(Handler)
        .await
        .expect("Err creating client");

    // Finally, start a single shard, and start listening to events.
    //
    // Shards will automatically attempt to reconnect, and will perform
    // exponential backoff until it reconnects.
    if let Err(why) = client.start().await {
        println!("Client error: {:?}", why);
    }
}
