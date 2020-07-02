import sys

import logging
from telegram import (ReplyKeyboardMarkup, ReplyKeyboardRemove,TelegramError)
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters,
                          ConversationHandler)
from telegram.error import (TelegramError, Unauthorized, BadRequest, 
                            TimedOut, ChatMigrated, NetworkError)                          

#Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

CHOICE, LOCATION, CHOOSING = range(3)

token = sys.argv[1]

def start(update, context):
    reply_keyboard = [['Stat by location','Global stat'],
                        ['Done']]

    update.message.reply_text(
        'Hi! My name is PandemicTrackerBot, ask me anything!',
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))

    return CHOICE


def choice(update, context):
    user = update.message.from_user
    logger.info("Choice of %s: %s", user.first_name, update.message.text)
    reply_keyboard = [['More stat','Done']]

    if update.message.text == 'Stat by location':
        update.message.reply_text('Send me a fucking location', reply_markup=ReplyKeyboardRemove())
        return LOCATION

    elif  update.message.text == 'Global stat':
        update.message.reply_text(
        'Here there are the data:\n... ... ...',
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))

        return CHOOSING


def location(update, context):
    user = update.message.from_user
    user_location = update.message.location
    logger.info("Location of %s: %f / %f", user.first_name, user_location.latitude,
                user_location.longitude)
    
    reply_keyboard = [['More stat','Done']]
    update.message.reply_text(
        'Here there are the data:\n... ... ...',
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))
    return CHOOSING

def choosing(update, context): 
    user = update.message.from_user
    logger.info("Chosing")
    reply_keyboard = [['Stat by location','Global stat'],
                        ['Done']]

    if update.message.text == 'More stat':
        update.message.reply_text(
                "I hope u are doing well",
                reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))
        return CHOICE

    elif update.message.text == 'Done':
        return ConversationHandler.END

    
def done(update, context):
    #user = update.message.from_user
    logger.info("End")
    update.message.reply_text('Bye!', reply_markup=ReplyKeyboardRemove())

    return ConversationHandler.END

def error_handler(update, context):
    try:
        raise context.error
    except Unauthorized:
        # remove update.message.chat_id from conversation list
        print("a")
    except BadRequest:
        # handle malformed requests - read more below!
        print("b")
    except TimedOut:
        # handle slow connection problems
        print("c")
    except NetworkError:
        # handle other connection problems
        print("d")
    except ChatMigrated as e:
        # the chat_id of a group has changed, use e.new_chat_id instead
        print("e")
    except TelegramError:
        # handle all other telegram related errors
        print("f")

def main():
    #use_context=True to use the new context based callbacks
    updater = Updater(token, use_context=True)

    dp = updater.dispatcher

    #conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],

        states={

            CHOICE: [MessageHandler(Filters.regex('^(Stat by location|Global stat)$'), choice)],

            LOCATION: [MessageHandler(Filters.location, location)],

            CHOOSING: [MessageHandler(Filters.regex('^More stat$'), choosing)]
        },

        fallbacks=[MessageHandler(Filters.regex('^Done$'), done)]
    )

    dp.add_handler(conv_handler)
    dp.add_error_handler(error_handler)

    #Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()