use std::sync::Arc;
use std::sync::Mutex;

use either::Either;
use pinyin::{Pinyin, ToPinyin};
use tbot::{
    contexts::{Command, Text},
    types::{
        input_file,
        parameters::{self, WebPagePreviewState},
    },
    Bot,
};

use crate::client::pull_feed;
use crate::constant::GLOBAL_ADMIN;
use crate::data::Database;
use crate::messages::{format_large_msg, Escape};

mod opml;

pub async fn check_command(owner: Option<i64>, cmd: Arc<Command<Text>>) -> bool {
    use tbot::contexts::fields::Message;
    let target = &mut MsgTarget::new(cmd.chat.id, cmd.message_id);
    let from = cmd
        .from()
        .map(|user| user.id.0)
        .unwrap_or_else(|| cmd.chat.id.0);
    if matches!(owner, Some(owner) if owner != from) {
        eprintln!(
            "Unauthenticated request from user/channel: {}, command: {}, args: {}",
            from, cmd.command, cmd.text.value
        );
        return false;
    }
    if cmd.chat().kind.is_channel() {
        let msg = tr!("commands_in_private_channel");
        let _ignore_result = update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await;
        return false;
    }

    true
}

#[derive(Debug, Copy, Clone)]
struct MsgTarget {
    chat_id: tbot::types::chat::Id,
    message_id: tbot::types::message::Id,
    first_time: bool,
}

impl MsgTarget {
    fn new(chat_id: tbot::types::chat::Id, message_id: tbot::types::message::Id) -> Self {
        MsgTarget {
            chat_id,
            message_id,
            first_time: true,
        }
    }
    fn update(&mut self, message_id: tbot::types::message::Id) {
        self.message_id = message_id;
        self.first_time = false;
    }
}

pub async fn start(
    _db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let target = &mut MsgTarget::new(cmd.chat.id, cmd.message_id);
    let msg = tr!("start_message");
    update_response(&cmd.bot, target, parameters::Text::markdown(&msg)).await?;
    Ok(())
}

pub async fn showset(
    db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let chat_id = cmd.chat.id;
    let text = &cmd.text.value;
    let args = text.split_whitespace().collect::<Vec<_>>();
    let mut target_id = chat_id;
    let target = &mut MsgTarget::new(chat_id, cmd.message_id);
    let feed_url;
    reject_cmd_from_channel!(cmd, target);

    match &*args {
        [url] => {
            feed_url = url;
        }
        [channel, url] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let channel_id = check_op_permission(&cmd.bot, channel, target, user_id).await?;
            if channel_id.is_none() {
                return Ok(());
            }
            target_id = channel_id.unwrap();
            feed_url = url;
        }
        [..] => {
            let msg = "使用方法: /showset [Channel ID] <RSS URL>";
            update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(());
        }
    };

    let setting_wraped = db.lock().unwrap().get_setting(target_id.0, &feed_url);
    if setting_wraped.is_none() {
        let msg = "找不到该订阅";
        update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
        return Ok(());
    }
    let setting = setting_wraped.unwrap();

    let msg = format!(
        "disable_preview: {} \n\
         link_only: {} \n\
         hide_rss_title: {} \n\
         combine_msg: {}",
        Escape(&setting.disable_preview.unwrap().to_string()),
        Escape(&setting.link_only.unwrap().to_string()),
        Escape(&setting.hide_rss_title.unwrap().to_string()),
        Escape(&setting.combine_msg.unwrap().to_string()),
    );

    update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;

    Ok(())
}

pub async fn set(
    db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let chat_id = cmd.chat.id;
    let chat_id_str = cmd.chat.id.to_string();
    let text = &cmd.text.value;
    let args = text.split_whitespace().collect::<Vec<_>>();
    let mut target_id = chat_id;
    let target = &mut MsgTarget::new(chat_id, cmd.message_id);
    let feed_url;
    let setting_key_value;
    reject_cmd_from_channel!(cmd, target);

    match &*args {
        [url, kv] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let result = check_op_permission(&cmd.bot, &chat_id_str, target, user_id).await?;
            if result.is_none() {
                return Ok(());
            }
            feed_url = url;
            setting_key_value = *kv;
        }
        [channel, url, kv] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let channel_id = check_op_permission(&cmd.bot, channel, target, user_id).await?;
            if channel_id.is_none() {
                return Ok(());
            }
            target_id = channel_id.unwrap();
            feed_url = url;
            setting_key_value = *kv;
        }
        [..] => {
            let msg = "使用方法: /set [Channel ID] <RSS URL> <key=value>";
            update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(());
        }
    };

    let setting_key_value_arr = setting_key_value.split("=").collect::<Vec<_>>();
    let setting_key;
    let setting_value;
    match *setting_key_value_arr {
        [key, value] => {
            setting_key = key;
            setting_value = value;
        }
        [..] => {
            let msg = "使用方法: /set [Channel ID] <RSS URL> <key=value>";
            update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(());
        }
    }

    let setting_wraped = db.lock().unwrap().get_setting(target_id.0, &feed_url);
    if setting_wraped.is_none() {
        let msg = "找不到该订阅";
        update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
        return Ok(());
    }
    let mut setting = setting_wraped.unwrap();
    let mut err = None;
    match setting_key {
        "disable_preview" => {
            let setting_value_parsed = setting_value.parse::<bool>();
            match setting_value_parsed {
                Ok(v) => setting.disable_preview = Some(v),
                Err(e) => err = Some(e.to_string()),
            }
        }
        "link_only" => {
            let setting_value_parsed = setting_value.parse::<bool>();
            match setting_value_parsed {
                Ok(v) => setting.link_only = Some(v),
                Err(e) => err = Some(e.to_string()),
            }
        }
        "hide_rss_title" => {
            let setting_value_parsed = setting_value.parse::<bool>();
            match setting_value_parsed {
                Ok(v) => setting.hide_rss_title = Some(v),
                Err(e) => err = Some(e.to_string()),
            }
        }
        "combine_msg" => {
            let setting_value_parsed = setting_value.parse::<bool>();
            match setting_value_parsed {
                Ok(v) => setting.combine_msg = Some(v),
                Err(e) => err = Some(e.to_string()),
            }
        }
        _ => {
            let msg = "没有此设置项";
            update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(());
        }
    }
    if (!err.is_none()) {
        let msg = format!("设置值错误 ({})", err.unwrap());
        update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
        return Ok(());
    }

    let msg = if db
        .lock()
        .unwrap()
        .update_setting(target_id.0, &feed_url, &setting)
    {
        "更改完成"
    } else {
        "更改失败"
    };
    update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;

    Ok(())
}

pub async fn rss(
    db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let chat_id = cmd.chat.id;
    let channel = &cmd.text.value;
    let mut target_id = chat_id;
    let target = &mut MsgTarget::new(chat_id, cmd.message_id);

    if !channel.is_empty() {
        let user_id = cmd.from.as_ref().unwrap().id;
        let channel_id = check_op_permission(&cmd.bot, channel, target, user_id).await?;
        if channel_id.is_none() {
            return Ok(());
        }
        target_id = channel_id.unwrap();
        if !target.first_time {
            cmd.bot
                .delete_message(target.chat_id, target.message_id)
                .call()
                .await?;
        }
    }

    let feeds = db.lock().unwrap().subscribed_feeds(target_id.0);
    let msgs = if let Some(mut feeds) = feeds {
        feeds.sort_by_cached_key(|feed| {
            feed.title
                .chars()
                .map(|c| {
                    c.to_pinyin()
                        .map(Pinyin::plain)
                        .map(Either::Right)
                        .unwrap_or_else(|| Either::Left(c))
                })
                .collect::<Vec<Either<char, &str>>>()
        });
        format_large_msg(tr!("subscription_list").to_string(), &feeds, |feed| {
            format!(
                "<a href=\"{}\">{}</a>",
                Escape(&feed.link),
                Escape(&feed.title)
            )
        })
    } else {
        vec![tr!("subscription_list_empty").to_string()]
    };

    let mut prev_msg = cmd.message_id;
    for msg in msgs {
        let text = parameters::Text::html(&msg);
        let msg = cmd
            .bot
            .send_message(chat_id, text)
            .reply_to_message_id(prev_msg)
            .web_page_preview(WebPagePreviewState::Disabled)
            .call()
            .await?;
        prev_msg = msg.id;
    }
    Ok(())
}

pub async fn sub(
    db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let chat_id = cmd.chat.id;
    let chat_id_str = chat_id.to_string();
    let text = &cmd.text.value;
    let args = text.split_whitespace().collect::<Vec<_>>();
    let mut target_id = chat_id;
    let target = &mut MsgTarget::new(chat_id, cmd.message_id);
    let feed_url;

    match &*args {
        [url] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let result = check_op_permission(&cmd.bot, &chat_id_str, target, user_id).await?;
            if result.is_none() {
                return Ok(());
            }
            feed_url = url
        }
        [channel, url] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let channel_id = check_op_permission(&cmd.bot, channel, target, user_id).await?;
            if channel_id.is_none() {
                return Ok(());
            }
            target_id = channel_id.unwrap();
            feed_url = url;
        }
        [..] => {
            let msg = tr!("sub_how_to_use");
            update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(());
        }
    };
    if db.lock().unwrap().is_subscribed(target_id.0, feed_url) {
        update_response(
            &cmd.bot,
            target,
            parameters::Text::plain(tr!("subscribed_to_rss")),
        )
        .await?;
        return Ok(());
    }

    if cfg!(feature = "hosted-by-iovxw") && db.lock().unwrap().all_feeds().len() >= 1500 {
        let msg = tr!("subscription_rate_limit");
        update_response(&cmd.bot, target, parameters::Text::markdown(msg)).await?;
        return Ok(());
    }
    update_response(
        &cmd.bot,
        target,
        parameters::Text::plain(tr!("processing_please_wait")),
    )
    .await?;
    let msg = match pull_feed(feed_url).await {
        Ok(feed) => {
            if db.lock().unwrap().subscribe(target_id.0, feed_url, &feed) {
                tr!(
                    "subscription_succeeded",
                    link = Escape(&feed.link),
                    title = Escape(&feed.title)
                )
            } else {
                tr!("subscribed_to_rss").into()
            }
        }
        Err(e) => tr!("subscription_failed", error = Escape(&e.to_user_friendly())),
    };
    update_response(&cmd.bot, target, parameters::Text::html(&msg)).await?;
    Ok(())
}

pub async fn unsub(
    db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let chat_id = cmd.chat.id;
    let chat_id_str = cmd.chat.id.to_string();
    let text = &cmd.text.value;
    let args = text.split_whitespace().collect::<Vec<_>>();
    let mut target_id = chat_id;
    let target = &mut MsgTarget::new(chat_id, cmd.message_id);
    let feed_url;

    match &*args {
        [url] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let result = check_op_permission(&cmd.bot, &chat_id_str, target, user_id).await?;
            if result.is_none() {
                return Ok(());
            }
            feed_url = url
        }
        [channel, url] => {
            let user_id = cmd.from.as_ref().unwrap().id;
            let channel_id = check_op_permission(&cmd.bot, channel, target, user_id).await?;
            if channel_id.is_none() {
                return Ok(());
            }
            target_id = channel_id.unwrap();
            feed_url = url;
        }
        [..] => {
            let msg = tr!("unsub_how_to_use");
            update_response(&cmd.bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(());
        }
    };
    let msg = if let Some(feed) = db.lock().unwrap().unsubscribe(target_id.0, feed_url) {
        tr!(
            "unsubscription_succeeded",
            link = Escape(&feed.link),
            title = Escape(&feed.title)
        )
    } else {
        tr!("unsubscribed_from_rss").into()
    };
    update_response(&cmd.bot, target, parameters::Text::html(&msg)).await?;
    Ok(())
}

pub async fn export(
    db: Arc<Mutex<Database>>,
    cmd: Arc<Command<Text>>,
) -> Result<(), tbot::errors::MethodCall> {
    let chat_id = cmd.chat.id;
    let channel = &cmd.text.value;
    let mut target_id = chat_id;
    let target = &mut MsgTarget::new(chat_id, cmd.message_id);

    if !channel.is_empty() {
        let user_id = cmd.from.as_ref().unwrap().id;
        let channel_id = check_op_permission(&cmd.bot, channel, target, user_id).await?;
        if channel_id.is_none() {
            return Ok(());
        }
        target_id = channel_id.unwrap();
    }

    let feeds = db.lock().unwrap().subscribed_feeds(target_id.0);
    if feeds.is_none() {
        update_response(
            &cmd.bot,
            target,
            parameters::Text::plain(tr!("subscription_list_empty")),
        )
        .await?;
        return Ok(());
    }
    let opml = opml::into_opml(feeds.unwrap());

    cmd.bot
        .send_document(
            chat_id,
            input_file::Document::bytes("feeds.opml", opml.as_bytes()),
        )
        .reply_to_message_id(cmd.message_id)
        .call()
        .await?;
    Ok(())
}

async fn update_response(
    bot: &Bot,
    target: &mut MsgTarget,
    message: parameters::Text<'_>,
) -> Result<(), tbot::errors::MethodCall> {
    let msg = if target.first_time {
        bot.send_message(target.chat_id, message)
            .reply_to_message_id(target.message_id)
            .web_page_preview(WebPagePreviewState::Disabled)
            .call()
            .await?
    } else {
        bot.edit_message_text(target.chat_id, target.message_id, message)
            .web_page_preview(WebPagePreviewState::Disabled)
            .call()
            .await?
    };
    target.update(msg.id);
    Ok(())
}

async fn check_op_permission(
    bot: &Bot,
    chat: &str,
    target: &mut MsgTarget,
    user_id: tbot::types::user::Id,
) -> Result<Option<tbot::types::chat::Id>, tbot::errors::MethodCall> {
    use tbot::errors::MethodCall;
    let chat_id = chat
        .parse::<i64>()
        .map(|id| parameters::ChatId::Id(id.into()))
        .unwrap_or_else(|_| parameters::ChatId::Username(chat));
    update_response(bot, target, parameters::Text::plain(tr!("verifying"))).await?;

    let chat = match bot.get_chat(chat_id).call().await {
        Err(MethodCall::RequestError {
            description,
            error_code: 400,
            ..
        }) => {
            let msg = tr!("unable_to_find_target_channel", desc = description);
            update_response(bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(None);
        }
        other => other?,
    };
    if chat.kind.is_private() {
        if chat.id.0 == user_id.0 {
            return Ok(Some(chat.id));
        } else {
            update_response(
                bot,
                target,
                parameters::Text::plain(tr!("target_cannot_be_other")),
            )
            .await?;
            return Ok(None);
        }
    }
    let admins = match bot.get_chat_administrators(chat_id).call().await {
        Err(MethodCall::RequestError {
            description,
            error_code: 400,
            ..
        }) => {
            let msg = tr!("unable_to_get_channel_info", desc = description);
            update_response(bot, target, parameters::Text::plain(&msg)).await?;
            return Ok(None);
        }
        other => other?,
    };
    let user_is_admin = admins
        .iter()
        .find(|member| member.user.id == user_id)
        .is_some();
    if !user_is_admin && !is_user_global_admin(user_id) {
        update_response(
            bot,
            target,
            parameters::Text::plain(tr!("admin_only_command")),
        )
        .await?;
        return Ok(None);
    }

    if chat.kind.is_channel() {
        let bot_is_admin = admins
            .iter()
            .find(|member| member.user.id == *crate::BOT_ID.get().unwrap())
            .is_some();
        if !bot_is_admin {
            update_response(bot, target, parameters::Text::plain(tr!("make_bot_admin"))).await?;
            return Ok(None);
        }
    }
    Ok(Some(chat.id))
}

fn is_user_global_admin(user_id: tbot::types::user::Id) -> bool {
    GLOBAL_ADMIN.contains(&user_id.0)
}
