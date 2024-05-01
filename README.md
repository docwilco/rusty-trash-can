# rusty-trash-can

A Discord bot that auto-deletes messages older than a certain time or going over maximum number of messages, or both. 

## Usage

1. Invite using: https://discord.com/oauth2/authorize?client_id=1093263866126942208&permissions=76800&scope=bot 
2. Control using `/autodelete` commands. You must have the `Manage Messages` permission to use these commands.
3. For private channels, the bot needs to be invited to the channel.

### `/autodelete start`

Start autodeletion or modify settings for a single channel. Takes two optional parameters (minimum of one):

 * `max_age`: Maximum age of messages. Example: `7d` for seven days. Supported unit suffixes:
    - `s`: seconds
    - `m`: minutes
    - `h`: hours
    - `d`: days
    - `w`: weeks
    - `mon`: months
    - `y`: years
 * `max_messages`: Maximum number of messages to keep. Excludes the bot's starting message.

Note that messages older than 14 days can not be deleted in bulk, and it might take a bit of time for the history to be deleted.

### `/autodelete stop`

Stop autodeletion on a channel.

### `/autodelete status`

Show the current autodeletion settings for a channel.

### `/autodelete help [<command>]`

Show help, optionally for a specific command.

## Running your own

1. Make an application on https://discord.com/developers/applications/
2. Make it a bot, copy the token
3. Copy `.env.example` to `.env` and fill in the token you got above
4. `cargo run`
5. Invite using `https://discord.com/oauth2/authorize?client_id=<app_id>&permissions=17179945984&scope=bot` (replacing `<app_id>` with your Discord App Id)

