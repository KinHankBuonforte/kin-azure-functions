const snowflake = require('snowflake-sdk');
const axios = require('axios').default;

const initSnowflakeConnection = () => {
    return new Promise((resolve, reject) => {
        var connection = snowflake.createConnection({
            account: 'ch10140.us-central1.gcp',
            username: 'HANKB',
            password: 'HankB123',
            application: 'COMPUTE_WH'
        });

        connection.connect((err, conn) => {
            if (err) {
                reject('Unable to connect: ' + err.message);
            } else {
                connection.execute({
                    sqlText: "USE ROLE ACCOUNTADMIN", complete: () => {
                        connection.execute({
                            sqlText: "USE Database ENERFLO", complete: () => {
                                connection.execute({
                                    sqlText: "USE schema PUBLIC", complete: () => {
                                        connection.execute({
                                            sqlText: "USE WAREHOUSE COMPUTE_WH", complete: () => {
                                                resolve(connection); 
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    });
}

const executeSql = (connection, sql) => {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: sql, complete: (err, stmt, rows) => {
                if (err) {
                    reject('Failed to execute statement due to the following error: ' + err.message);
                } else {
                    resolve(rows);
                }
            }
        })
    });
}

const fetchUsersV2 = async () => {
  const res = await fetch(
    "https://enerflo.io/company/users/get?take=25&skip=0&page=1&pageSize=1000&format=json",
    {
      headers: {
        accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language":
          "en-US,en;q=0.9,ko-KR;q=0.8,ko;q=0.7,zh-CN;q=0.6,zh;q=0.5",
        "cache-control": "no-cache",
        pragma: "no-cache",
        "sec-ch-ua":
          '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        cookie:
          "ajs_anonymous_id=aeba403c-98c8-43b1-a3b0-6130f593303c; intercom-id-eooublx2=bf9d1951-7257-40e2-a3c1-a99e04c73481; intercom-device-id-eooublx2=c910a306-0024-4058-8113-c950d1dd1df1; remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6IjUyWWNPZ2xuRG80WmVLOG9Oc0dVN3c9PSIsInZhbHVlIjoiblUxcU1tQTNCU2t3SGduY0ZPQWlUeHdqaXdjQnFhWGo0WFExUzk5Vk5WeEZPNCtadFdDM2RcL2IyOFU1ejdiWU80NFVJWGR4d0NpWFl3K0xwT2hTUzNocXFmb2gwNHA0TXNHZkt2QTJ4QU82ZGdUdXBZckRiRlpyeHhRV1hqbXkwWThEcEpla1pyNGFUbzlHb0JRTzB6Qm9QbnpMXC8zbTlYZ1pzT054b3JCbDg9IiwibWFjIjoiMmE3MjgwMzkxNTMyODYzNGM5YzY3ZDI2NmYzYzU3M2ZjYWQ4MzE3MmFlODZhOWJjYmMxMDUwZTM5NjEwYWRiZiJ9; enerflo_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjQwYmI1NDg3ZjE3YzFjNTk2ZjM5MmIwY2NmNTdkZmQ3ZTlkNTU3ZDFiNWEzZjAwYTMyMjQ0Yjc1NDJiNmJiNWEwNWFhYzIxODBhNzBkM2FlIn0.eyJhdWQiOiIxIiwianRpIjoiNDBiYjU0ODdmMTdjMWM1OTZmMzkyYjBjY2Y1N2RmZDdlOWQ1NTdkMWI1YTNmMDBhMzIyNDRiNzU0MmI2YmI1YTA1YWFjMjE4MGE3MGQzYWUiLCJpYXQiOjE2ODk4NzE3NzUsIm5iZiI6MTY4OTg3MTc3NSwiZXhwIjoxNzIxNDk0MTc1LCJzdWIiOiI1OTczMyIsInNjb3BlcyI6WyIqIl19.fcC7SbxiLmLXfjWBMjnIET3sVhJXNVg3Kggjr29-E1caJ6yGMw0LQ7NZJQJprk1IRDcx9ynVGSl-g9X9oMdBv_OOOcUwUEiIIl136TvXWvDH9NUCRdQ5J8naJjOJ6pgETb7fgSpspsdJXmUx4uW1SWsvp3h_BpBwmVDrdCGpCYPgFtxzMr8lsVk9PfpSAoGyUyMwWaJ53ChzP5nroZbm65kIMEQ24cnVvMwbc0PvblqM3jRdMkefFKqy8G4EmkaTClUFs82FkGWLzqR3_26gJYPlJp296TFqWuQZJXoiMoCWyk3u643ZmwmmsbIqf5IQKxqYX_XJ4idApeGGgyg2hma0XZxDQE1tNeB9zhly1YCYQ7NDTsEqiiktRrJW-qmnMyCRiRYsjKRoXKpvmey8OTQdWMSrwmm9ujJOWWfJEOmC8u2TxWMkFujOLKq2ysfiwX9zy4D-5MuH7K9SAm-2dyGplzOTs6isPQvc5m6mUgcpIFtBMV5BeieJ5Fz9Q4_LQtp5lZmh-FXe3M5AhOkIrXhdg0tXKX16sxrlXT97R8EYnHf9-vm-YktANMagoOHSotMIxpcKPYxqohI7NlsqjlAgcob9eRO1n8LAd5w_sQEITDDEUDwBt_sKDhoOCxJU-H7W2NKDflrFjtOPiZX-Xkxsd1yg8RCg0RG-o1vnnpc; enerflo2_token_prod=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjQwYmI1NDg3ZjE3YzFjNTk2ZjM5MmIwY2NmNTdkZmQ3ZTlkNTU3ZDFiNWEzZjAwYTMyMjQ0Yjc1NDJiNmJiNWEwNWFhYzIxODBhNzBkM2FlIn0.eyJhdWQiOiIxIiwianRpIjoiNDBiYjU0ODdmMTdjMWM1OTZmMzkyYjBjY2Y1N2RmZDdlOWQ1NTdkMWI1YTNmMDBhMzIyNDRiNzU0MmI2YmI1YTA1YWFjMjE4MGE3MGQzYWUiLCJpYXQiOjE2ODk4NzE3NzUsIm5iZiI6MTY4OTg3MTc3NSwiZXhwIjoxNzIxNDk0MTc1LCJzdWIiOiI1OTczMyIsInNjb3BlcyI6WyIqIl19.fcC7SbxiLmLXfjWBMjnIET3sVhJXNVg3Kggjr29-E1caJ6yGMw0LQ7NZJQJprk1IRDcx9ynVGSl-g9X9oMdBv_OOOcUwUEiIIl136TvXWvDH9NUCRdQ5J8naJjOJ6pgETb7fgSpspsdJXmUx4uW1SWsvp3h_BpBwmVDrdCGpCYPgFtxzMr8lsVk9PfpSAoGyUyMwWaJ53ChzP5nroZbm65kIMEQ24cnVvMwbc0PvblqM3jRdMkefFKqy8G4EmkaTClUFs82FkGWLzqR3_26gJYPlJp296TFqWuQZJXoiMoCWyk3u643ZmwmmsbIqf5IQKxqYX_XJ4idApeGGgyg2hma0XZxDQE1tNeB9zhly1YCYQ7NDTsEqiiktRrJW-qmnMyCRiRYsjKRoXKpvmey8OTQdWMSrwmm9ujJOWWfJEOmC8u2TxWMkFujOLKq2ysfiwX9zy4D-5MuH7K9SAm-2dyGplzOTs6isPQvc5m6mUgcpIFtBMV5BeieJ5Fz9Q4_LQtp5lZmh-FXe3M5AhOkIrXhdg0tXKX16sxrlXT97R8EYnHf9-vm-YktANMagoOHSotMIxpcKPYxqohI7NlsqjlAgcob9eRO1n8LAd5w_sQEITDDEUDwBt_sKDhoOCxJU-H7W2NKDflrFjtOPiZX-Xkxsd1yg8RCg0RG-o1vnnpc; ajs_user_id=59733; _ga_7VLDT0ST7T=GS1.2.1691085185.30.0.1691085185.60.0.0; _gid=GA1.2.690103073.1699297948; _ga=GA1.1.1866432734.1689871205; _ga_3K2G1DR21N=GS1.1.1699297947.46.1.1699297980.27.0.0; intercom-session-eooublx2=aWg0MldaSFlRQUxZTk01N3pFSFBLVjJLV0VocUV2TDlZdGtRbTVIbDFORzdGRXpkdTBTck5rMXRnaXUxMUVhbi0tSGdwYmZXUFJZSnBqUFVuczJyeTlPdz09--ebd6275be00516b7324e77470743d704426c6a9d; laravel_token=eyJpdiI6IlRtTXRJMkR0dW1WZGVDR21WaDFKQ2c9PSIsInZhbHVlIjoiY21rR2Z5eGhkS0dKbDU0UTlQdDFwWHVjeXpCMFJaR01yaitXYUZBMkx2djhyczBac0RkcmJkUEtCUlJNbVY2ck9mRXZJektyRzBrWmo2NXptSTc4R0p5RUJVSU5JMzJ2ODNTRFlVTkkyRjJjdXhXd2N2VERJNVRlQjFkaFZ5UXpKZU9QYXBGdlwvcWUxUlVkMmM1SmR4ZGE2WUxMSnRLY052dGVCb2lScU9NMjdsVXJNT2VSdjNKVnQxZ1M1ekhybytlMTRVeUptSjh3K2htb0lCczBNT2dcLzROV1c4RmNaNFJwWVpHMnV2MURCd2M5bklYRnV2QW9JdndCTFMxQ1pJZWlFdTl2MlwvblRjZXFGT1VmNWxKUlE9PSIsIm1hYyI6ImQyMmFmNmUwZWJlYzkyMDk0NjViNjE5ZTEwZWZiMThiMmRjMzVkZmMxN2RkMjQwMGM3ZTFkMjkyNDk2MTQyMGMifQ%3D%3D; XSRF-TOKEN=eyJpdiI6IkVOZUloalBER3JaU05lR1RrZHpEaGc9PSIsInZhbHVlIjoiOVo2a1RYQWQzekxmZk50YzViZXhGdzludmJQRjBRTm1NbU9qMUxEXC9pa0dzamFzSGhHd3YxSmhXdjdaS0x4QTMiLCJtYWMiOiJiN2ZjYThiZWFkZTY4M2Q5ZmY1ZjNkNThlMTkzOWMxODYyMGNiNzE2MmRjM2I0ZjJjZTM2YTI1N2IwN2VkNjhjIn0%3D; enerflo_session=eyJpdiI6IitNOFR6aGRLWk5BY2ZzUFppNTdLU2c9PSIsInZhbHVlIjoiY0tuVWRBUERJTmpSY0NVdDY4Qmo4dXJNWGpMRHFJamJyaTNiVUV0d05HOU5pWklBM1VcLzFWK0lHc1E4aUJ1a1MiLCJtYWMiOiJmYjg0YjIyNDI2Mjk4NDgwNjE4MjY2ODhhMmEwNDBmMWJiNWYzZGZjYjU5MDUzYzc1ZTMyMmFlNzE3MTBlNDRiIn0%3D",
      },
      referrerPolicy: "strict-origin-when-cross-origin",
      body: null,
      method: "GET",
    }
  );
  return await res.json();
};

const fetchOffices = async () => {
  const connection = await initSnowflakeConnection();

  try {
    console.log("Fetching FIKA data...");

    const { data: officeRes } = await axios.get(
      `https://enerflo.io/api/v3/offices?api_key=13686046e8dc420946.70185370`
    );
    let { data: userRes } = await axios.get(
      `https://enerflo.io/api/v3/users?api_key=13686046e8dc420946.70185370`
    );
    let userV2Res = await fetchUsersV2();
    const inserted_at = new Date().toUTCString();

    console.log("Creating Offices Table...");

    const OFFCIES_TABLE_QUERY = `
          create or replace TABLE ENERFLO.PUBLIC."ENERFLO_OFFICES" (
              "id" NUMBER(10,0) UNIQUE PRIMARY KEY,
              "created_at" VARCHAR(16777216),
              "name" VARCHAR(16777216),
              "phone" VARCHAR(16777216),
              "address" VARCHAR(16777216),
              "city" VARCHAR(16777216),
              "state" VARCHAR(16777216),
              "zip" VARCHAR(16777216),
              "crm_id" VARCHAR(16777216),
              "inserted_at" VARCHAR(16777216)
          );
      `;
    await executeSql(connection, OFFCIES_TABLE_QUERY);

    console.log("Inserting Offices Data...");

    const cols = [
      "id",
      "created_at",
      "name",
      "phone",
      "address",
      "city",
      "state",
      "zip",
      "crm_id",
    ];
    const OFFICES_INSERT_QUERY = `
          INSERT OVERWRITE INTO ENERFLO.PUBLIC."ENERFLO_OFFICES" (${cols
            .map((col) => `"${col}"`)
            .join(",")},"inserted_at")
          VALUES
          ${officeRes.results
            .map(
              (r) =>
                `(${cols
                  .map((col) =>
                    r[col] ? (col === "id" ? r.id : `'${r[col]}'`) : `null`
                  )
                  .join(",")},'${inserted_at}')`
            )
            .join(",")};
      `;
    await executeSql(connection, OFFICES_INSERT_QUERY);

    console.log("Creating Users Table...");

    const USERS_TABLE_QUERY = `
          create or replace TABLE ENERFLO.PUBLIC."ENERFLO_USERS" (
              "id" NUMBER(10,0) UNIQUE PRIMARY KEY,
              "created_at" VARCHAR(65535),
              "name" VARCHAR(65535),
              "email" VARCHAR(65535),
              "phone" VARCHAR(65535),
              "last_name" VARCHAR(65535),
              "first_name" VARCHAR(65535),
              "timezone" VARCHAR(65535),
              "active" BOOLEAN,
              "external_user_id" VARCHAR(65535),
              "company_id" NUMBER(10,0),
              "office_ids" VARIANT,
              "roles" VARIANT,
              "manager" VARIANT,
              "inserted_at" VARCHAR(65535)
          );
      `;

    await executeSql(connection, USERS_TABLE_QUERY);

    console.log("Inserting Users Data...");

    const userColumns = [
      "id",
      "created_at",
      "name",
      "email",
      "phone",
      "last_name",
      "first_name",
      "timezone",
      "active",
      "external_user_id",
      "company_id",
      "office_ids",
      "roles",
      "manager",
    ];

    const usersData = userRes.results
      .map((r) => {
        return `SELECT ${userColumns
          .map((col) => {
            if (!r[col]) {
              return "null";
            }
            if (["id", "active", "company_id"].includes(col)) {
              return r[col];
            }
            if (["office_ids", "roles"].includes(col)) {
              return `[${r[col].map((item) => `'${item}'`).join(",")}]`;
            }
            if (col === "manager") {
              const m = r.manager;
              return `{'id': ${m.id}, 'first_name': '${m.first_name}', 'last_name': '${m.last_name}', 'email': '${m.email}', 'phone': '${m.phone}'}`;
            }
            return `'${r[col]}'`;
          })
          .join(",")}, '${inserted_at}'`;
      })
      .join(" UNION ALL ");

    const USERS_INSERT_QUERY = `
        INSERT OVERWRITE INTO ENERFLO.PUBLIC.ENERFLO_USERS
          (${userColumns.map((col) => `"${col}"`).join(",")}, "inserted_at")
        ${usersData}
      `;
    await executeSql(connection, USERS_INSERT_QUERY, []);

    console.log("Creating Users V2 Table...");

    const USERS_V2_TABLE_QUERY = `
          create or replace TABLE ENERFLO.PUBLIC."ENERFLO_USERS_V2" (
              "id" NUMBER(10,0) UNIQUE PRIMARY KEY,
              "created_at" VARCHAR(65535),
              "name" VARCHAR(65535),
              "email" VARCHAR(65535),
              "phone" VARCHAR(65535),
              "office" VARCHAR(65535),
              "inactive" BOOLEAN,
              "agent_office" VARCHAR(65535),
              "combined_offices" VARCHAR(65535),
              "roles" VARIANT,
              "last_login_date" VARCHAR(65535),
              "inserted_at" VARCHAR(65535)
          );
      `;

    await executeSql(connection, USERS_V2_TABLE_QUERY);

    console.log("Inserting Users V2 Data...");

    const userV2Columns = [
      "id",
      "created_at",
      "name",
      "email",
      "phone",
      "inactive",
      "agent_office",
      "office",
      "combined_offices",
      "roles",
      "last_login_date",
    ];

    const usersV2Data = userV2Res.data
      .map((r) => {
        return `SELECT ${userV2Columns
          .map((col) => {
            if (!r[col]) {
              return "null";
            }
            if (["id", "inactive"].includes(col)) {
              return r[col];
            }
            if (["roles"].includes(col)) {
              return `[${r[col].map((item) => `'${item}'`).join(",")}]`;
            }
            return `'${r[col]}'`;
          })
          .join(",")}, '${inserted_at}'`;
      })
      .join(" UNION ALL ");

    const USERS_V2_INSERT_QUERY = `
        INSERT OVERWRITE INTO ENERFLO.PUBLIC.ENERFLO_USERS_V2
          (${userV2Columns.map((col) => `"${col}"`).join(",")}, "inserted_at")
        ${usersV2Data}
      `;
    await executeSql(connection, USERS_V2_INSERT_QUERY);

    console.log("Finished FIKA data");
  } catch (err) {
    console.error(err);
  }
  await connection.destroy();
};

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);

  await fetchOffices();
};