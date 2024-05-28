const { initSnowflakeConnection } = require("../snowflake");

module.exports = async function (context, req) {
  context.log("JavaScript HTTP trigger function processed a request.");

  const body = req.body;

  const ID = new Date().getTime().toString();
  const PARAMS = JSON.stringify(body);
  const conn = await initSnowflakeConnection();

  await executeSql(
    conn,
    `INSERT INTO ENERFLO_WEBHOOK_EVENTS_NEW (ID, PARAMS, PROCESSED) VALUES ('${ID}', '${PARAMS}', false)`
  );

  context.res = {
    status: 200,
  };
};
