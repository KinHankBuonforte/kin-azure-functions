const { gql, GraphQLClient } = require("graphql-request");
const snowflake = require("snowflake-sdk");
const snakecase = require("snakecase");

const client = new GraphQLClient("https://kinhome.enerflo.io/graphql", {
  headers: {
    authorization:
      "bearer c42ed5acf0114730fd4130d1a19f7e816ae7985249cd1f9d47d4958b55f1690c",
  },
});

const fetchDeals = async () => {
  let page = 1;
  const records = [];

  while (1) {
    const document = gql`
      query MyQuery($page: Float!) {
        fetchDealList(input: { pageSize: 300, page: $page }) {
          currentPage
          pageSize
          totalItemCount
          totalPageCount
          items {
            createdAt
            updatedAt
            currentStage
            currentSubStage
            dispositionNotes
            customer {
              firstName
              lastName
              id
              email
            }
            disposition {
              id
              label
              status
            }
            id
            progress
            shortCode
            state
            status
            templateVersionIsValid
            updatedAt
            writable
            org {
              id
              name
              subdomain
              status
            }
            installer {
              id
              name
              status
              isActive
            }
          }
        }
      }
    `;
    const { fetchDealList: res } = await client.request(document, { page });
    records.push(...res.items);

    if (res.currentPage === res.totalPageCount) {
      break;
    }
    page += 1;
  }
  console.log(records.length);

  const connection = await initSnowflakeConnection();
  const tableName = "ENERFLO_DEALS";
  const flattenRecords = records.map((x) => flattenObject(x));
  const columns = await initTable(connection, tableName, flattenRecords);

  for (let i = 0; i < flattenRecords.length; i += 100) {
    console.log(`Inserting ${i} batch from ${flattenRecords.length}`);
    const values = flattenRecords
      .slice(i, i + 100)
      .map(
        (record) =>
          `(${columns.map((x) => sqlString(record[x.field])).join(", ")})`
      )
      .join(", ");
    await executeSql(
      connection,
      `INSERT INTO ${tableName} (${columns
        .map((c) => c.column)
        .join(", ")}) VALUES ${values}`
    );
  }
};

const flattenObject = (obj) => {
  const flattened = {};

  for (let key of Object.keys(obj)) {
    const value = obj[key];

    if (typeof value === "object" && !Array.isArray(value) && value !== null) {
      for (let valueKey of Object.keys(value)) {
        const valueValue = value[valueKey];
        if (
          (typeof valueValue === "object" || Array.isArray(valueValue)) &&
          valueValue !== null
        ) {
          flattened[key + "_" + valueKey] = JSON.stringify(valueValue);
        } else {
          flattened[key + "_" + valueKey] = valueValue;
        }
      }
    } else if (Array.isArray(value)) {
      flattened[key] = JSON.stringify(value);
    } else {
      flattened[key] = value;
    }
  }

  return flattened;
};

const initTable = async (connection, tableName, records) => {
  let columns = [];
  for (let key of Object.keys(records[0])) {
    const value = records[0][key];
    columns.push({
      name: key,
      value,
    });
  }
  for (let column of columns) {
    if (typeof column.value == "boolean") {
      column.type = "BOOLEAN";
    } else if (typeof column.value == "number") {
      column.type = "FLOAT";
    } else {
      column.type = "VARCHAR";
    }
  }
  console.log(`CREATE TABLE IF NOT EXISTS ${tableName} (${columns
    .map((x) => `${snakecase(x.name).toUpperCase()} ${x.type}`)
    .join(", ")})`);
  await executeSql(
    connection,
    `CREATE OR REPLACE TABLE ${tableName} (${columns
      .map((x) => `${snakecase(x.name).toUpperCase()} ${x.type}`)
      .join(", ")})`
  );
  return columns.map((x) => ({
    field: x.name,
    column: snakecase(x.name).toUpperCase(),
  }));
};

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

const sqlString = (value) => {
  if (typeof value == "string") {
    return `'${value.replace(/'/g, '"')}'`;
  } else if (typeof value == "number") {
    return value.toString();
  } else if (!value) {
    return "NULL";
  } else {
    return value.toString();
  }
};

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);

  try {
    await fetchDeals();

    context.res = {
      status: 200 /* Defaults to 200 */,
    };
  } catch (err) {
    console.error(err);
    throw err;
  }
};
