require("dotenv").config();
const moment = require("moment");
const { EmailClient } = require("@azure/communication-email");
const juice = require("juice");
const { initSnowflakeConnection, executeSql } = require("../snowflake");

const dbTables = [
  {
    db: "ENERFLO",
    tables: [
      "ENERFLO_APPOINTMENTS",
      "ENERFLO_CUSTOMERS",
      "ENERFLO_DEALS",
      "ENERFLO_INSTALLS",
      "ENERFLO_OFFICES",
      "ENERFLO_USERS",
      "ENERFLO_USERS_V2",
      // "ENERFLO_WEBHOOK_EVENTS",
      "ENERFLO_WEBHOOK_EVENTS_NEW",
    ],
  },
  {
    db: "ENZY_KIN",
    tables: ["ENZY_APPOINTMENTS", "ENZY_LEADS", "SFTP_DATA"],
  },
  {
    db: "QUICKBASE",
    tables: [
      "QUICKBASE_DESIGNS",
      "QUICKBASE_INSPECTIONS",
      "QUICKBASE_PROJECTS",
      "QUICKBASE_RETENSION",
      "QUICKBASE_SALES_AID",
      "QUICKBASE_INTAKE_EVENTS",
    ],
  },
];

async function checkTablesLastUpdate(context) {
  const untouchedTables = [];

  for (const database of dbTables) {
    const { db, tables } = database;

    try {
      const connection = await initSnowflakeConnection(db);
      const lastAlterDates = await executeSql(
        connection,
        `SELECT TABLE_NAME, LAST_ALTERED FROM ${db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN (${tables
          .map((t) => `'${t}'`)
          .join(",")})`
      );

      for (const { LAST_ALTERED, TABLE_NAME } of lastAlterDates) {
        const timeSince = moment()
          .utc()
          .diff(moment(LAST_ALTERED).utc(), "minutes");
        context.log(`${TABLE_NAME}: Last update ${timeSince} minutes ago`);
        if (timeSince > 120) {
          untouchedTables.push({
            table: TABLE_NAME,
            timeSince,
          });
        }
      }
      connection.destroy();
    } catch (err) {
      context.error(err);
    }
  }
  context.log(untouchedTables);

  if (!untouchedTables.length) {
    return;
  }
  await sendAlert(untouchedTables);
}

async function sendAlert(tables) {
  const connectionString =
    process.env.ALERT_COMMUNICATION_SERVICES_CONNECTION_STRING;
  const emailClient = new EmailClient(connectionString);
  const poller = await emailClient.beginSend({
    content: {
      subject: "KIN Snowflake Tables were not updated",
      html: juice(`
        <html>
          <head>
            <style>
              table, th, td {
                border-collapse: collapse;
                border: 1px solid;
                padding: 12px;
                font-family: sans-serif;
              }
            </style>
          </head>
          <table>
            <thead>
              <tr>
                <th>Table Name</th>
                <th>Last Updated</th>
              </tr>
            </thead>
            <tbody>
              ${tables
                .map(
                  (t) =>
                    `<tr>
                    <td>${t.table}</td>
                    <td>${t.timeSince} minutes ago</td>
                  </tr>
                `
                )
                .join("")}
            </tbody>
          </table>
        </html>
      `),
    },
    senderAddress: process.env.ALERT_SENDER_EMAIL,
    recipients: {
      to: process.env.ALERT_RECIPIENTS.split(",").map((address) => ({
        address,
      })),
    },
  });
  const result = await poller.pollUntilDone();

  if (result.error) {
    throw result.error.message;
  }
}

module.exports = async function (context, myTimer) {
  await checkTablesLastUpdate(context);
};
