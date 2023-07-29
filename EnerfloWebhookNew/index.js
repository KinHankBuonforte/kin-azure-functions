const snowflake = require('snowflake-sdk');

const initSnowflakeConnection = () => {
  return new Promise((resolve, reject) => {
    var connection = snowflake.createConnection({
      account: "ch10140.us-central1.gcp",
      username: "HANKB",
      password: "HankB123",
      application: "COMPUTE_WH",
    });

    connection.connect((err, conn) => {
      if (err) {
        reject("Unable to connect: " + err.message);
      } else {
        connection.execute({
          sqlText: "USE ROLE ACCOUNTADMIN",
          complete: () => {
            connection.execute({
              sqlText: "USE Database ENERFLO",
              complete: () => {
                connection.execute({
                  sqlText: "USE schema PUBLIC",
                  complete: () => {
                    connection.execute({
                      sqlText: "USE WAREHOUSE COMPUTE_WH",
                      complete: () => {
                        resolve(connection);
                      },
                    });
                  },
                });
              },
            });
          },
        });
      }
    });
  });
};

const executeSql = (connection, sql) => {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(
            "Failed to execute statement due to the following error: " +
              err.message
          );
        } else {
          resolve(rows);
        }
      },
    });
  });
};

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
