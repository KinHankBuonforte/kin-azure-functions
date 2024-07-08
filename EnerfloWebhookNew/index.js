const { Snowflake } = require("../snowflake");

module.exports = async function (context, req) {
  context.log("JavaScript HTTP trigger function processed a request.");

  const body = req.body;

  const ID = new Date().getTime().toString();
  const PARAMS = JSON.stringify(body);
  const snowflake = await Snowflake.create(context);
  context.log(
    new Date().toISOString(),
    `INSERT INTO ENERFLO_WEBHOOK_EVENTS_NEW (ID, PARAMS, PROCESSED, INSERTED_AT) VALUES ('${ID}', '${PARAMS}', false, '${new Date().toISOString()}')`
  );

  await snowflake.execute(
    `INSERT INTO ENERFLO_WEBHOOK_EVENTS_NEW (ID, PARAMS, PROCESSED, INSERTED_AT) VALUES ('${ID}', '${PARAMS}', false, '${new Date().toISOString()}')`
  );

  context.res = {
    status: 200,
  };
};

// module.exports(console, {
//   body: {
//     webhook_event: "update_install",
//     id: 4410202,
//     status: { id: 323, name: "Pending KCA" },
//     sales_company_id: 1368,
//     install_company_id: 1368,
//     office_id: 1368,
//     market_id: 102381,
//     deal_id: 2202525,
//     agreement_id: 570254,
//     customer: {
//       id: 2563892,
//       external_id: null,
//       first_name: "Craig",
//       last_name: "wright",
//       full_name: "Craig wright",
//       email: "craig.wright@mchsi.com",
//       phone: "(319) 530-7546",
//       address: {
//         street: "1705 Randall Dr",
//         city: "Solon",
//         state: "IA",
//         zip: "52333",
//         county: "Johnson County",
//         full_address: "1705 Randall Dr Solon, IA 52333",
//         latitude: "41.81174380",
//         longitude: "-91.50438230",
//       },
//       customer_timezone: "America/Chicago",
//       lead_source: "enzy",
//       timestamps: {
//         deleted_at: null,
//         created_at: "2024-06-08 20:18:48",
//         updated_at: "2024-06-08 23:55:18",
//         timezone: "UTC",
//       },
//     },
//     agent: {
//       id: 19188,
//       first_name: "Tyce",
//       last_name: "Stevens",
//       full_name: "Tyce Stevens",
//       email: "tyce.stevens@kinhome.com",
//       phone: "8014049947",
//       timezone: "America/Chicago",
//       deleted_at: null,
//       created_at: "2021-11-08 19:40:21",
//       updated_at: "2024-06-08 23:51:18",
//     },
//     timestamps: {
//       deleted_at: null,
//       created_at: "2024-06-08 23:39:30",
//       updated_at: "2024-06-08 23:55:18",
//       timezone: "UTC",
//     },
//   },
// });
