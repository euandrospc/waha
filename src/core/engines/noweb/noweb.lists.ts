import { generateWAMessageFromContent, proto } from '@adiwajshing/baileys';
import { Row, Section, SendListMessage } from '@waha/structures/chatting.list.dto';

export function randomId() {
  return Math.floor(Math.random() * 1000000000).toString();
}

function rowToJson(row: Row) {
  return {
    title: row.title,
    description: row.description || '',
    rowId: row.rowId,
  };
}

function sectionToJson(section: Section) {
  return {
    title: section.title,
    rows: section.rows.map(rowToJson),
  };
}

export async function sendListMessage(
  sock: any,
  chatId: string,
  listMessage: SendListMessage,
): Promise<any> {
  const data = {
    viewOnceMessage: {
      message: {
        messageContextInfo: {
          deviceListMetadata: {},
          deviceListMetadataVersion: 2,
        },
        listMessage: {
          title: listMessage.title,
          description: listMessage.description,
          buttonText: listMessage.button,
          footerText: listMessage.footer,
          listType: 'SINGLE_SELECT',
          sections: listMessage.sections.map(sectionToJson),
        },
      },
    },
  };

  const msg = proto.Message.fromObject(data);
  const fullMessage = generateWAMessageFromContent(chatId, msg, {
    userJid: sock?.user?.id,
  });
  await sock.relayMessage(chatId, fullMessage.message, {
    messageId: fullMessage.key.id,
  });
  return fullMessage;
}
