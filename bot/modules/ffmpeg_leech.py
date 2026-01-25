#!/usr/bin/env python3
from pyrogram.handlers import MessageHandler
from pyrogram.filters import command

from bot import bot, DOWNLOAD_DIR, LOGGER, config_dict, user_data
from bot.helper.telegram_helper.message_utils import sendMessage, delete_links
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.listeners.tasks_listener import MirrorLeechListener
from bot.helper.mirror_utils.download_utils.ffmpeg_download import FFmpegDownloader
from bot.helper.ext_utils.bot_utils import is_url, arg_parser, new_task, fetch_user_dumps
from bot.helper.ext_utils.task_manager import task_utils
from bot.helper.telegram_helper.button_build import ButtonMaker


@new_task
async def ffmpeg_leech(client, message):
    text = message.text.split('\n')
    input_list = text[0].split(' ')
    
    arg_base = {
        'link': '',
        '-n': '', '-name': '',
        '-ud': '', '-dump': '',
    }
    
    args = arg_parser(input_list[1:], arg_base)
    
    link = args['link']
    name = args['-n'] or args['-name']
    user_dump = args['-ud'] or args['-dump']
    
    if username := message.from_user.username:
        tag = f'@{username}'
    else:
        tag = message.from_user.mention

    if not link and (reply_to := message.reply_to_message) and reply_to.text:
        link = reply_to.text.split('\n', 1)[0].strip()

    if not is_url(link):
        await sendMessage(message, f'{tag} Please provide a valid URL.\n\nUsage: /ffl [URL] -n [name]\n\nExample:\n<code>/ffl https://example.com/video.m3u8 -n MyVideo</code>')
        await delete_links(message)
        return

    error_msg = []
    error_button = None
    task_utilis_msg, error_button = await task_utils(message)
    if task_utilis_msg:
        error_msg.extend(task_utilis_msg)

    if error_msg:
        final_msg = f'Hey, <b>{tag}</b>,\n'
        for __i, __msg in enumerate(error_msg, 1):
            final_msg += f'\n<b>{__i}</b>: {__msg}\n'
        if error_button is not None:
            error_button = error_button.build_menu(2)
        await sendMessage(message, final_msg, error_button)
        await delete_links(message)
        return

    # Handle leech dump destination
    up = None
    if user_dump and (user_dump.isdigit() or user_dump.startswith('-')):
        up = int(user_dump)
    elif user_dump and user_dump.startswith('@'):
        up = user_dump
    elif (ldumps := await fetch_user_dumps(message.from_user.id)):
        if user_dump and user_dump.casefold() == "all":
            up = [dump_id for dump_id in ldumps.values()]
        elif user_dump:
            up = next((dump_id for name_, dump_id in ldumps.items() if user_dump.casefold() == name_.casefold()), '')
        if not up and len(ldumps) == 1:
            up = next(iter(ldumps.values()))

    path = f'{DOWNLOAD_DIR}{message.id}'
    
    listener = MirrorLeechListener(
        message, 
        isLeech=True, 
        tag=tag, 
        upPath=up,
        source_url=link
    )

    await delete_links(message)
    LOGGER.info(f'FFmpeg Leech: {link}')
    
    ffmpeg_dl = FFmpegDownloader(listener)
    await ffmpeg_dl.add_download(link, path, name)


bot.add_handler(MessageHandler(ffmpeg_leech, filters=command(
    BotCommands.FFmpegLeechCommand) & CustomFilters.authorized & ~CustomFilters.blacklisted))
