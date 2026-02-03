# pip install pyrogram
# pip install tgcrypto
from os import getenv

from dotenv import load_dotenv
from pyrogram import Client

# from PIL import Image, ImageOps

import logging

load_dotenv()
# checa libs
# for name in logging.root.manager.loggerDict:
    # if "gi" in name.lower():
    # print(name)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

# app = Client("DDTipstelbot", bot_token="")
app = Client(getenv('TELEGRAM_CLIENT'))
chat_id = getenv('TELEGRAM_CHAT_ID')


def enviar_no_telegram(chat_id=chat_id, msg='You need to set a message', img_path = None) -> int:
    """
        Enviando mensagem e salva o ID no banco
    """
    send_without_img = True
    app.start()
    if img_path is not None: # if exists img path
        try:
            # fix img before send
            # img = Image.open(img_path).convert("RGB")
            # img_path_pil = ImageOps.expand(img, border=(0, 50, 0, 50), fill=(255, 255, 255))  
            # img_path_pil.save(img_path, format="PNG")

            msg = app.send_photo(
                chat_id,
                photo=img_path, # Caminho local ou URL da imagem
                caption=msg,
            )
            send_without_img = False
        except Exception as error:
            logging.error(error)
    
    if send_without_img:
        msg = app.send_message(chat_id, msg)

    id = msg.id
    app.stop()
    return id


async def resultado_da_entrada(chat_id, reply_msg_id, msg):
    """
        responde a msg de entrada com o resultado(green/red)
    """
    await app.start()
    await app.send_message(chat_id, f'{msg}', reply_to_message_id=reply_msg_id)
    await app.stop()

# app.run(resultado_da_entrada(chat_id, reply_msg_id, msg))

# função assíncrona
@app.on_message() # quando receber uma mensagem...
async def resposta(client, message): 
    print(message.chat.id, message.text) #Pessoa, oq a pessoa diz ao bot
    # await message.reply('me sorry yo no hablo tu language D:') # resposta do bot

# app.run() # executa

if __name__ == '__main__':
    with app: # achar os chats
        for dialog in app.get_dialogs():
            chat = dialog.chat
            if str(chat.type) == 'ChatType.CHANNEL':
                print(f"{chat.title} | tipo: {chat.type} | username: {chat.id}")

msg = '⚽️⏰ ONFIRE 🔥'
enviar_no_telegram(chat_id=getenv('TELEGRAM_CHAT_ID'), msg=msg)