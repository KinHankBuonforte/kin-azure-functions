const { Snowflake } = require("../snowflake");
const moment = require("moment");
const { EmailClient } = require("@azure/communication-email");
const juice = require("juice");

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
      const snowflake = await Snowflake.create(context, db);
      const lastAlterDates = await snowflake.execute(
        `SELECT TABLE_NAME, LAST_ALTERED FROM ${
          snowflake.dbName
        }.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN (${tables
          .map((t) => `'${t}'`)
          .join(",")})`
      );

      for (const table of tables) {
        try {
          const records = await snowflake.execute(
            `SELECT INSERTED_AT FROM ${table} WHERE INSERTED_AT IS NOT NULL ORDER BY INSERTED_AT DESC LIMIT 1`
          );
          const tableRecord = lastAlterDates.find(
            (t) => t.TABLE_NAME === table
          );
          const record = records[0];

          if (!tableRecord) {
            throw new Error("Table does not exist");
          }
          tableRecord.LAST_RECORD_INSERTED_AT = record?.INSERTED_AT ?? null;
        } catch (err) {
          context.error("Getting INSERTED_AT error:", err);
        }
      }

      for (const {
        LAST_ALTERED,
        LAST_RECORD_INSERTED_AT,
        TABLE_NAME,
      } of lastAlterDates) {
        const timeSinceAlter = moment()
          .utc()
          .diff(moment(LAST_ALTERED).utc(), "minutes");

        context.log(`${TABLE_NAME}: Last update ${timeSinceAlter} minutes ago`);

        const timeSinceInsert = LAST_RECORD_INSERTED_AT
          ? moment()
              .utc()
              .diff(moment(LAST_RECORD_INSERTED_AT).utc(), "minutes")
          : 0;

        context.log(
          `${TABLE_NAME}: Last insert ${timeSinceInsert} minutes ago`
        );

        if (timeSinceAlter > 120 || LAST_RECORD_INSERTED_AT > 120) {
          untouchedTables.push({
            table: TABLE_NAME,
            timeSinceAlter,
            timeSinceInsert,
          });
        }
      }
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
                <th>Last Record Inserted</th>
              </tr>
            </thead>
            <tbody>
              ${tables
                .map(
                  (t) =>
                    `<tr>
                    <td>${t.table}</td>
                    <td>${t.timeSinceAlter} minutes ago</td>
                    <td>${t.timeSinceInsert ?? 0} minutes ago</td>
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

// module.exports(console, {});
