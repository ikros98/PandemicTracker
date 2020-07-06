import sys
from time import sleep
import logging
from telegram import (ReplyKeyboardMarkup, ReplyKeyboardRemove, TelegramError, Bot, ChatAction)
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters,
                          ConversationHandler)
from telegram.error import (TelegramError, Unauthorized, BadRequest, 
                            TimedOut, ChatMigrated, NetworkError)                          
from query import *
from graph import *

#Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

CHOICE, LOCATION, CHOOSING = range(3)

token = sys.argv[1]
#use_context=True to use the new context based callbacks
updater = Updater(token, use_context=True)
bot = Bot(token = token)

def start(update, context):
    reply_keyboard = [['Dati dalla posizione','Esci']]

    update.message.reply_text(
        'Ciao! il mio nome è PandemicTrackerBot, come posso aiutarti?',
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))

    return CHOICE

def choice(update, context):
    user = update.message.from_user
    logger.info("Choice of %s: %s", user.first_name, update.message.text)
    reply_keyboard = [['Altri Dati','Esci']]

    if update.message.text == 'Dati dalla posizione':
        update.message.reply_text('Condividimi una posizione', reply_markup=ReplyKeyboardRemove())
        return LOCATION

        return CHOOSING

def location(update, context):
    user = update.message.from_user
    user_location = update.message.location
    logger.info("Location of %s: %f / %f", user.first_name, user_location.latitude,
                user_location.longitude)

    bot.sendChatAction(chat_id=update.message.chat_id, action=ChatAction.TYPING)
    
    province = get_province_for(user_location.latitude, user_location.longitude)
    station = get_station_for(user_location.latitude, user_location.longitude)
    observations = get_observations_for(province[0], station[0])

    image = plot_for(province, station, observations)
    bot.send_photo(chat_id=update.message.chat_id, photo=image)

    reply_keyboard = [['Altri Dati','Esci']]
    update.message.reply_text(
        'Spero che tu stia bene',
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))

    return CHOOSING

def choosing(update, context): 
    user = update.message.from_user
    logger.info("Chosing")
    reply_keyboard = [['Dati dalla posizione','Esci']]

    if update.message.text == 'Altri Dati':
        update.message.reply_text(
                "Chiedimi pure!",
                reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=True))
        return CHOICE

    elif update.message.text == 'Esci':
        return ConversationHandler.END

    
def done(update, context):
    # user = update.message.from_user
    logger.info("End")
    update.message.reply_text('Alla prossima!', reply_markup=ReplyKeyboardRemove())

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

    dp = updater.dispatcher

    # conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],

        states={

            CHOICE: [MessageHandler(Filters.regex('^(Dati dalla posizione)$'), choice)],

            LOCATION: [MessageHandler(Filters.location, location)],

            CHOOSING: [MessageHandler(Filters.regex('^Altri Dati$'), choosing)]
        },

        fallbacks=[MessageHandler(Filters.regex('^Esci$'), done)]
    )

    dp.add_handler(conv_handler)
    dp.add_error_handler(error_handler)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()